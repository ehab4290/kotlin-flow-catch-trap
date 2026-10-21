@file:OptIn(ExperimentalCoroutinesApi::class, FlowPreview::class)

package catchtrap.operators

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.sample
import kotlinx.coroutines.flow.timeout
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.flow.zip
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.combineTransform
import kotlinx.coroutines.flow.distinctUntilChangedBy
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEmpty
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.runningReduce
import kotlinx.coroutines.flow.scan
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.transformLatest
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.flow.withIndex
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import catchtrap.flowcatch.newFakeMainDispatcher
import kotlin.time.Duration.Companion.seconds

/*
 * Which operators, placed between a failing source and `catch`, break `catch`?
 *
 * For every operator this measures two things:
 *
 * 1. "runs ahead": does the source's emit() return before the collector has finished with the value?
 *    In a plain flow it never does. With a channel in between it does, and from then on the source
 *    and the collector can fail independently.
 *
 * 2. What `op(source).catch { emit("fallback") }` does when the source emits "cached" and then fails.
 */

class UpstreamError : RuntimeException("upstream failed")

/**
 * @param dropsByDesign the operator is allowed to drop values (conflate, debounce, the *Latest family),
 *   so a lost value is not, on its own, evidence of a bug.
 */
class OperatorCase(
    val name: String,
    val dropsByDesign: Boolean = false,
    val apply: (Flow<String>) -> Flow<String>,
)

fun operatorCases(collectorDispatcher: CoroutineDispatcher): List<OperatorCase> = listOf(
    // ---- expected: no channel. The source and the collector take turns in one coroutine. ----
    OperatorCase("(nothing)") { it },
    OperatorCase("map { }") { it.map { v -> v } },
    OperatorCase("mapNotNull { }") { it.mapNotNull { v -> v } },
    OperatorCase("filter { }") { it.filter { true } },
    OperatorCase("filterNot { }") { it.filterNot { false } },
    OperatorCase("filterNotNull()") { it.filterNotNull() },
    OperatorCase("filterIsInstance<String>()") { it.filterIsInstance<String>() },
    OperatorCase("onEach { }") { it.onEach { } },
    OperatorCase("onStart { }") { it.onStart { } },
    OperatorCase("onCompletion { }") { it.onCompletion { } },
    OperatorCase("onEmpty { }") { it.onEmpty { } },
    OperatorCase("transform { }") { it.transform { v -> emit(v) } },
    OperatorCase("transformWhile { }") { it.transformWhile { v -> emit(v); true } },
    OperatorCase("take(5)") { it.take(5) },
    OperatorCase("takeWhile { true }") { it.takeWhile { true } },
    OperatorCase("drop(0)") { it.drop(0) },
    OperatorCase("dropWhile { false }") { it.dropWhile { false } },
    OperatorCase("distinctUntilChanged()") { it.distinctUntilChanged() },
    OperatorCase("distinctUntilChangedBy { }") { it.distinctUntilChangedBy { v -> v } },
    OperatorCase("withIndex()") { it.withIndex().map { v -> v.value } },
    OperatorCase("scan(initial) { }  (= runningFold)") { it.scan("") { _, v -> v }.filter { v -> v.isNotEmpty() } },
    OperatorCase("runningReduce { }") { it.runningReduce { _, v -> v } },
    OperatorCase("chunked(1)") { it.chunked(1).map { chunk -> chunk.single() } },
    OperatorCase("cancellable()") { it.cancellable() },
    OperatorCase("retryWhen { false }") { it.retryWhen { _, _ -> false } },
    OperatorCase("catch { throw it }") { it.catch { e -> throw e } },
    OperatorCase("flatMapConcat { source }") { src -> flowOf(Unit).flatMapConcat { src } },
    OperatorCase("flattenConcat()") { src -> flowOf(src).flattenConcat() },
    OperatorCase("flatMapMerge(concurrency = 1) { source }") { src -> flowOf(Unit).flatMapMerge(concurrency = 1) { src } },
    OperatorCase("source.zip(other)") { it.zip(flowOf(1, 2)) { a, _ -> a } },
    OperatorCase("flowOn(collector's own dispatcher)") { it.flowOn(collectorDispatcher) },
    OperatorCase("flowOn(CoroutineName(\"repo\"))") { it.flowOn(CoroutineName("repo")) },

    // ---- expected: a channel. The source runs in its own coroutine. ----
    OperatorCase("flowOn(Dispatchers.IO)") { it.flowOn(Dispatchers.IO) },
    OperatorCase("flowOn(another dispatcher, same threads)") { it.flowOn(SameThreads(collectorDispatcher)) },
    OperatorCase("buffer()") { it.buffer() },
    OperatorCase("buffer(RENDEZVOUS)") { it.buffer(Channel.RENDEZVOUS) },
    OperatorCase("conflate()", dropsByDesign = true) { it.conflate() },
    OperatorCase("mapLatest { }", dropsByDesign = true) { it.mapLatest { v -> v } },
    OperatorCase("transformLatest { }", dropsByDesign = true) { it.transformLatest { v -> emit(v) } },
    OperatorCase("flatMapLatest { source }", dropsByDesign = true) { src -> flowOf(Unit).flatMapLatest { src } },
    OperatorCase("flatMapMerge { source }") { src -> flowOf(Unit).flatMapMerge { src } },
    OperatorCase("flattenMerge()") { src -> flowOf(src).flattenMerge() },
    OperatorCase("merge(source, other)") { merge(it, emptyFlow()) },
    OperatorCase("source.combine(other)") { it.combine(flowOf(1)) { a, _ -> a } },
    OperatorCase("other.combine(source)") { flowOf(1).combine(it) { _, b -> b } },
    OperatorCase("source.combineTransform(other) { }") { it.combineTransform(flowOf(1)) { a, _ -> emit(a) } },
    OperatorCase("other.zip(source)") { flowOf(1, 2).zip(it) { _, b -> b } },
    OperatorCase("debounce(1)", dropsByDesign = true) { it.debounce(1) },
    OperatorCase("sample(1)", dropsByDesign = true) { it.sample(1) },
    OperatorCase("timeout(1.seconds)") { it.timeout(1.seconds) },
    OperatorCase("channelFlow { source.collect { send(it) } }") { src ->
        channelFlow { src.collect { send(it) } }
    },
    OperatorCase("callbackFlow { source.collect { trySend(it) } }") { src ->
        callbackFlow { src.collect { trySend(it) }; close(); awaitClose { } }
    },
)

/** A different dispatcher object that runs everything on [delegate]'s threads, like Main and Main.immediate. */
private class SameThreads(private val delegate: CoroutineDispatcher) : CoroutineDispatcher() {
    override fun dispatch(context: CoroutineContext, block: Runnable) = delegate.dispatch(context, block)
}

data class Tally(var ok: Int = 0, var lost: Int = 0, var escaped: Int = 0) {
    override fun toString(): String = String.format("ok %3d lost %3d escaped %3d", ok, lost, escaped)
}

data class OperatorRow(
    val case: OperatorCase,
    val ranAhead: Int,
    val probes: Int,
    val first: Tally,
    val toList: Tally,
    val suspendingCollector: Tally,
    val toListFixed: Tally,
    val suspendingCollectorFixed: Tally,
)

/** Emits, then fails with no suspension point in between. */
private fun failsImmediately(): Flow<String> = flow {
    emit("cached")
    throw UpstreamError()
}

/** Emits, then fails 2 ms later from a suspending call, like a Retrofit `suspend fun`. */
private fun failsAfterSuspending(): Flow<String> = flow {
    emit("cached")
    delay(2)
    throw UpstreamError()
}

private suspend fun probeRunsAhead(case: OperatorCase, probes: Int): Int {
    var ahead = 0
    repeat(probes) {
        var processed = false
        var ranAhead = false
        val source = flow {
            emit("cached")
            ranAhead = !processed // did emit() come back before the collector was done?
        }
        runCatching {
            withTimeout(2_000) {
                case.apply(source).collect {
                    yield()
                    processed = true
                }
            }
        }
        if (ranAhead) ahead++
    }
    return ahead
}

private suspend fun tally(runs: Int, attempt: suspend () -> Boolean): Tally {
    val tally = Tally()
    repeat(runs) {
        try {
            if (withTimeout(2_000) { attempt() }) tally.ok++ else tally.lost++
        } catch (e: UpstreamError) {
            tally.escaped++
        }
    }
    return tally
}

suspend fun measure(case: OperatorCase, runs: Int): OperatorRow {
    val probes = 20
    return OperatorRow(
        case = case,
        ranAhead = probeRunsAhead(case, probes),
        probes = probes,
        first = tally(runs) {
            case.apply(failsImmediately()).catch { emit("fallback") }.first() == "cached"
        },
        toList = tally(runs) {
            "cached" in case.apply(failsImmediately()).catch { emit("fallback") }.toList()
        },
        suspendingCollector = tally(runs) {
            val seen = ArrayList<String>(2)
            case.apply(failsAfterSuspending()).catch { emit("fallback") }.collect {
                seen += it
                delay(10) // suspending work per value: a database write, an animation, a snackbar
            }
            "cached" in seen
        },
        toListFixed = tally(runs) {
            // The fix: catch sits before the operator, next to the source.
            "cached" in case.apply(failsImmediately().catch { emit("fallback") }).toList()
        },
        suspendingCollectorFixed = tally(runs) {
            val seen = ArrayList<String>(2)
            case.apply(failsAfterSuspending().catch { emit("fallback") }).collect {
                seen += it
                delay(10)
            }
            "cached" in seen
        },
    )
}

/** Runs the whole matrix twice: collecting on a single main thread, then on a thread pool. */
fun runOperatorMatrix(runs: Int, print: (String) -> Unit = ::println): List<OperatorRow> {
    val rows = mutableListOf<OperatorRow>()
    newFakeMainDispatcher().use { main ->
        val contexts = listOf(
            "collector on a single main thread" to main,
            "collector on Dispatchers.Default (a thread pool)" to Dispatchers.Default,
        )
        for ((label, dispatcher) in contexts) {
            print("")
            print("==== $label")
            print(
                String.format(
                    "%-50s %-10s | %-28s | %-28s | %-28s | %-28s | %-28s",
                    "operator between the source and catch", "runs ahead",
                    "catch { }.first()", "catch { }.toList()",
                    "catch { }.collect { delay }", "FIX: toList()", "FIX: collect { delay }",
                )
            )
            for (case in operatorCases(dispatcher)) {
                val row = runBlocking(dispatcher) { measure(case, runs) }
                print(
                    String.format(
                        "%-50s %-10s | %-28s | %-28s | %-28s | %-28s | %-28s%s",
                        case.name, "${row.ranAhead}/${row.probes}",
                        row.first, row.toList, row.suspendingCollector, row.toListFixed, row.suspendingCollectorFixed,
                        if (case.dropsByDesign) "  (drops values by design)" else "",
                    )
                )
                rows += row
            }
        }
    }
    return rows
}
