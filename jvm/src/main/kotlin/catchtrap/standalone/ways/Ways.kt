package catchtrap.standalone.ways

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import catchtrap.fixes.catchAware
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

fun newFakeMainDispatcher() = Executors.newSingleThreadExecutor { r -> Thread(r, "fake-main").apply { isDaemon = true } }.asCoroutineDispatcher()

/*
 * The ways-out table again, with every outcome counted separately:
 *   rethrown          the upstream error reached the caller
 *   cached            the caller received "cached"
 *   fallback          the caller received "fallback"
 *   catch ran         the catch / catchAware block ran
 * Same timings as FixOptions: collector suspends 10 ms, source fails 2 ms after emitting (suspending),
 * first() with a 3 ms blocking call, toList-style collector with an immediate failure.
 *
 * Two more sections follow the table: the other early stops with catch and catchAware, and the safer
 * way's limit, catch right before flowOn while the error still gets through it.
 */

class UpstreamError : RuntimeException("upstream failed")
enum class Failure { AFTER_SUSPENDING, AFTER_BLOCKING, IMMEDIATE }

suspend fun fail(f: Failure): Nothing {
    when (f) { Failure.AFTER_SUSPENDING -> delay(2); Failure.AFTER_BLOCKING -> Thread.sleep(3); Failure.IMMEDIATE -> Unit }
    throw UpstreamError()
}
fun source(f: Failure) = flow { emit("cached"); fail(f) }

class Outcome {
    var rethrown = 0; var cachedDelivered = 0; var noErrorCachedLost = 0; var catchRan = 0
    override fun toString() = "error reached the caller %3d | cached delivered %3d | no error but cached lost %3d | catch block ran %3d"
        .format(rethrown, cachedDelivered, noErrorCachedLost, catchRan)
}
class LimitOutcome {
    var rethrown = 0; var workCancelled = 0; var cachedLost = 0; var fallbackDelivered = 0; var catchRan = 0
    override fun toString() = "error reached the caller %3d | work on cached cancelled %3d | cached lost %3d | fallback delivered %3d | catch block ran %3d"
        .format(rethrown, workCancelled, cachedLost, fallbackDelivered, catchRan)
}
class FallbackMapperError : RuntimeException("mapping the fallback failed")
class StopOutcome {
    var rethrown = 0; var returnedCached = 0; var returnedOther = 0; var catchRan = 0
    override fun toString() = "rethrown %3d | returned the cached value %3d | returned something else %3d | catch block ran %3d"
        .format(rethrown, returnedCached, returnedOther, catchRan)
}

class Opt(val name: String, val build: (Failure, CoroutineDispatcher, AtomicInteger) -> Flow<String>,
          val neverSuspends: Boolean = false, val callerCatches: Boolean = false)

val io = Dispatchers.IO
val opts = listOf(
    Opt("catch after flowOn (the trap)", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") } }),
    Opt("catch before flowOn", { f, d, c -> source(f).catch { c.incrementAndGet(); emit("fallback") }.flowOn(d) }),
    Opt("try/catch inside the flow builder", { f, d, c -> flow { emit("cached"); try { fail(f) } catch (e: UpstreamError) { c.incrementAndGet(); emit("fallback") } }.flowOn(d) }),
    Opt("no flowOn: withContext inside the flow", { f, d, c -> flow { emit(withContext(d) { "cached" }); withContext(d) { fail(f) } }.catch { c.incrementAndGet(); emit("fallback") } }),
    Opt("buffer() after catch", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") }.buffer() }),
    Opt("collector never suspends", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") } }, neverSuspends = true),
    Opt("try/catch around the terminal call", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") } }, callerCatches = true),
    Opt("catchAware", { f, d, c -> source(f).flowOn(d).catchAware { c.incrementAndGet(); emit("fallback") } }),
)

suspend fun one(o: Opt, situation: Int, out: Outcome) {
    val seen = ArrayList<String>()
    val catchRan = AtomicInteger()
    var rethrown = false
    try {
        when (situation) {
            0 -> { val fl = o.build(Failure.AFTER_SUSPENDING, io, catchRan)
                   if (o.neverSuspends) coroutineScope { fl.collect { seen += it; launch { delay(10) } } }
                   else fl.collect { seen += it; delay(10) } }
            1 -> seen += o.build(Failure.AFTER_BLOCKING, io, catchRan).first()
            2 -> o.build(Failure.IMMEDIATE, io, catchRan).collect { seen += it }
        }
    } catch (e: UpstreamError) { rethrown = true }
    if (rethrown) out.rethrown++
    if ("cached" in seen) out.cachedDelivered++
    if (!rethrown && "cached" !in seen) out.noErrorCachedLost++
    if (catchRan.get() > 0) out.catchRan++
}

fun main(args: Array<String>) {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    println("kotlinx.coroutines 1.11.0, $runs runs per cell")
    newFakeMainDispatcher().use { main ->
        runBlocking(main) {
            for (o in opts) {
                println("== ${o.name}")
                for ((i, label) in listOf("collector suspends", "first(), blocking call", "toList(), immediate failure").withIndex()) {
                    val out = Outcome(); repeat(runs) { one(o, i, out) }
                    println("   %-28s %s".format(label, out))
                }
            }

            // catchAware and plain catch with the other early stops, blocking call (like the first() column)
            println("== early stops after flowOn, source fails after a 3 ms blocking call")
            val stops = listOf<Pair<String, suspend (Flow<String>) -> Any?>>(
                "first()" to { f -> f.first() },
                "firstOrNull()" to { f -> f.firstOrNull() },
                "take(1).toList()" to { f -> f.take(1).toList() },
                "any { true }" to { f -> f.any { true } },
                "first { true }" to { f -> f.first { true } },
            )
            for ((opName, build) in listOf<Pair<String, (AtomicInteger) -> Flow<String>>>(
                "catch" to { c -> source(Failure.AFTER_BLOCKING).flowOn(io).catch { c.incrementAndGet(); emit("fallback") } },
                "catchAware" to { c -> source(Failure.AFTER_BLOCKING).flowOn(io).catchAware { c.incrementAndGet(); emit("fallback") } },
            )) for ((stopName, stop) in stops) {
                val out = StopOutcome()
                repeat(runs) {
                    val c = AtomicInteger()
                    try {
                        val r = stop(build(c))
                        if (r == "cached" || r == listOf("cached") || (r == true && c.get() == 0)) out.returnedCached++ else out.returnedOther++
                    } catch (e: UpstreamError) { out.rethrown++ }
                    if (c.get() > 0) out.catchRan++
                }
                println("   %-11s %-18s %s".format(opName, stopName, out))
            }

            // The safer way holds only while catch's block doesn't throw. catch sits right before flowOn in
            // every row; in the last two the error still gets through it, and the producer fails again.
            println("== catch before flowOn, and the error still gets through it")
            val limits = listOf<Pair<String, (Failure, AtomicInteger) -> Flow<String>>>(
                "catch handles it" to { f, c -> source(f).catch { c.incrementAndGet(); emit("fallback") }.flowOn(io) },
                "catch lets it through" to { f, c ->
                    source(f).catch { c.incrementAndGet(); if (it is java.io.IOException) emit("fallback") else throw it }.flowOn(io) },
                "map after catch throws" to { f, c ->
                    source(f).catch { c.incrementAndGet(); emit("fallback") }
                        .map { if (it == "fallback") throw FallbackMapperError() else it }.flowOn(io) },
            )
            for ((limitName, build) in limits) {
                for ((suspends, label) in listOf(true to "collector suspends", false to "toList(), immediate failure")) {
                    val out = LimitOutcome()
                    repeat(runs) {
                        val c = AtomicInteger()
                        val seen = ArrayList<String>()
                        val finished = ArrayList<String>()
                        try {
                            if (suspends) build(Failure.AFTER_SUSPENDING, c).collect { seen += it; delay(10); finished += it }
                            else build(Failure.IMMEDIATE, c).collect { seen += it; finished += it }
                        } catch (e: UpstreamError) { out.rethrown++ } catch (e: FallbackMapperError) { out.rethrown++ }
                        if ("cached" in seen && "cached" !in finished) out.workCancelled++
                        if ("cached" !in seen) out.cachedLost++
                        if ("fallback" in seen) out.fallbackDelivered++
                        if (c.get() > 0) out.catchRan++
                    }
                    println("   %-23s %-28s %s".format(limitName, label, out))
                }
            }
        }
    }
}
