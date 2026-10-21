package catchtrap.standalone.latest

import catchtrap.standalone.ways.UpstreamError
import catchtrap.standalone.ways.newFakeMainDispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.transformLatest
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/*
 * Where does collectLatest { } fit: part of the trap, or a way out?
 *
 * Everything is collected on one main thread. The source emits, then fails on Dispatchers.IO.
 *
 * 1. Four situations, with catch after flowOn (the trap) and with catch before flowOn (the safer way):
 *    - suspending work, catch emits a fallback: the source fails 2 ms after emitting, the work takes 10 ms;
 *    - suspending work, catch only logs: the same, but catch emits nothing, as in the article's chat;
 *    - a value in the queue: the source fails right after emitting, the collector does no work;
 *    - old work slow to stop: the source emits "a", then "b" 2 ms later, then fails 2 ms after that.
 *      The work is a 10 ms blocking call inside withContext(IO), so it can't stop until the call returns.
 *    Each is collected with collect { }, conflate().collect { }, collectLatest { }, and the other operators
 *    built on the same code as collectLatest: mapLatest, transformLatest, flatMapLatest.
 *    Besides the usual counts, it counts whether the work on the first value finished or was cancelled.
 * 2. The other cases of the trap, with catch after flowOn, in a scope with a handler (viewModelScope plus a
 *    crash counter):
 *    - case 3: the scope is cancelled (the user leaves) while the source is in a blocking call that then fails;
 *    - case 4: the collector's work throws its own exception while the source fails.
 */

private const val WORK_MS = 10L

class Counts {
    var rethrown = 0; var catchRan = 0; var firstSeen = 0; var firstWorkFinished = 0; var fallbackSeen = 0; var lostSilently = 0
    override fun toString() = "error reached the caller %3d | catch ran %3d | first value received %3d | work on first value finished %3d | fallback received %3d | no error but first value lost %3d"
        .format(rethrown, catchRan, firstSeen, firstWorkFinished, fallbackSeen, lostSilently)
}

@OptIn(ExperimentalCoroutinesApi::class)
enum class Collect(val label: String, val run: suspend (Flow<String>, suspend (String) -> Unit) -> Unit) {
    COLLECT("collect { }", { f, a -> f.collect { a(it) } }),
    CONFLATE("conflate().collect { }", { f, a -> f.conflate().collect { a(it) } }),
    LATEST("collectLatest { }", { f, a -> f.collectLatest { a(it) } }),
    MAP_LATEST("mapLatest { }.collect()", { f, a -> f.mapLatest { a(it) }.collect() }),
    TRANSFORM_LATEST("transformLatest { }.collect()", { f, a -> f.transformLatest<String, Unit> { a(it) }.collect() }),
    FLAT_MAP_LATEST("flatMapLatest { }.collect()", { f, a -> f.flatMapLatest { v -> flow<Unit> { a(v) } }.collect() }),
}

enum class Situation(val label: String, val values: List<String>, val fallback: Boolean) {
    WORK_FALLBACK("suspending work, catch emits a fallback", listOf("cached"), true),
    WORK_LOG_ONLY("suspending work, catch only logs", listOf("cached"), false),
    IN_QUEUE("a value in the queue", listOf("cached"), true),
    SLOW_TO_STOP("a newer value while the old work is slow to stop", listOf("a", "b"), false),
}

private fun source(s: Situation) = flow {
    for ((i, v) in s.values.withIndex()) { if (i > 0) delay(2); emit(v) }
    if (s != Situation.IN_QUEUE) delay(2)
    throw UpstreamError()
}

private fun trap(s: Situation, catchRan: AtomicInteger): Flow<String> =
    source(s).flowOn(Dispatchers.IO).catch { catchRan.incrementAndGet(); if (s.fallback) emit("fallback") }

private fun safer(s: Situation, catchRan: AtomicInteger): Flow<String> =
    source(s).catch { catchRan.incrementAndGet(); if (s.fallback) emit("fallback") }.flowOn(Dispatchers.IO)

private fun work(s: Situation, seen: MutableList<String>, done: MutableList<String>): suspend (String) -> Unit = { v ->
    seen += v
    when (s) {
        Situation.IN_QUEUE -> Unit
        Situation.SLOW_TO_STOP -> withContext(Dispatchers.IO) { Thread.sleep(WORK_MS) }
        else -> delay(WORK_MS)
    }
    done += v
}

private suspend fun situation(build: (Situation, AtomicInteger) -> Flow<String>, s: Situation, how: Collect, runs: Int): Counts {
    val out = Counts()
    val first = s.values.first()
    repeat(runs) {
        val c = AtomicInteger(); val seen = CopyOnWriteArrayList<String>(); val done = CopyOnWriteArrayList<String>()
        var rethrown = false
        try { how.run(build(s, c), work(s, seen, done)) } catch (e: UpstreamError) { rethrown = true }
        if (rethrown) out.rethrown++
        if (c.get() > 0) out.catchRan++
        if (first in seen) out.firstSeen++
        if (first in done) out.firstWorkFinished++
        if ("fallback" in seen) out.fallbackSeen++
        if (!rethrown && first !in seen) out.lostSilently++
    }
    return out
}

class Bug : RuntimeException("the collector's own bug")

class CaseCounts {
    var crashed = 0; var crashedWithUpstream = 0; var crashedWithOwnBug = 0; var catchRan = 0
    override fun toString() = "crashed %3d (upstream error %3d, the collector's own exception %3d) | catch ran %3d"
        .format(crashed, crashedWithUpstream, crashedWithOwnBug, catchRan)
}

/** Case 3: the scope is cancelled 2 ms in; the source is in a 5 ms blocking call, then fails. */
private suspend fun case3(how: Collect, runs: Int, main: CoroutineDispatcher): CaseCounts {
    val out = CaseCounts()
    repeat(runs) {
        val errors = CopyOnWriteArrayList<Throwable>()
        val c = AtomicInteger()
        val scope = CoroutineScope(SupervisorJob() + main + CoroutineExceptionHandler { _, e -> errors += e })
        val flow = flow { emit("cached"); Thread.sleep(5); throw UpstreamError() }
            .flowOn(Dispatchers.IO).catch { c.incrementAndGet(); emit("fallback") }
        scope.launch { how.run(flow) { delay(WORK_MS) } }
        delay(2); scope.cancel(); delay(30)
        if (errors.isNotEmpty()) { out.crashed++; if (errors.first() is UpstreamError) out.crashedWithUpstream++ else out.crashedWithOwnBug++ }
        if (c.get() > 0) out.catchRan++
    }
    return out
}

/** Case 4: the work on the cached value throws after 1 ms; the source fails 1 ms after emitting. */
private suspend fun case4(how: Collect, runs: Int, main: CoroutineDispatcher): CaseCounts {
    val out = CaseCounts()
    repeat(runs) {
        val errors = CopyOnWriteArrayList<Throwable>()
        val c = AtomicInteger()
        val scope = CoroutineScope(SupervisorJob() + main + CoroutineExceptionHandler { _, e -> errors += e })
        val flow = flow { emit("cached"); delay(1); throw UpstreamError() }
            .flowOn(Dispatchers.IO).catch { c.incrementAndGet(); emit("fallback") }
        scope.launch { how.run(flow) { v -> if (v == "cached") { delay(1); throw Bug() } } }
        delay(30); scope.cancel()
        if (errors.isNotEmpty()) { out.crashed++; if (errors.first() is UpstreamError) out.crashedWithUpstream++ else out.crashedWithOwnBug++ }
        if (c.get() > 0) out.catchRan++
    }
    return out
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    println("kotlinx.coroutines 1.11.0, $runs runs per row, collected on one main thread")
    newFakeMainDispatcher().use { main ->
        runBlocking(main) {
            for ((title, build) in listOf<Pair<String, (Situation, AtomicInteger) -> Flow<String>>>(
                "catch after flowOn (the trap)" to ::trap,
                "catch before flowOn (the safer way)" to ::safer,
            )) {
                for (s in Situation.entries) {
                    println("== $title, ${s.label}")
                    for (how in Collect.entries) println("   %-30s %s".format(how.label, situation(build, s, how, runs)))
                }
            }
            println("== the other cases, catch after flowOn, in a scope with a handler")
            for (how in listOf(Collect.COLLECT, Collect.CONFLATE, Collect.LATEST)) {
                println("   %-30s case 3, user leaves      %s".format(how.label, case3(how, runs, main)))
                println("   %-30s case 4, collector throws %s".format(how.label, case4(how, runs, main)))
            }
        }
    }
}
