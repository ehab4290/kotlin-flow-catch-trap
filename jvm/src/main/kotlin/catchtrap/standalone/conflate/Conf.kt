package catchtrap.standalone.conflate

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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
class StopOutcome {
    var rethrown = 0; var returnedCached = 0; var returnedOther = 0; var catchRan = 0
    override fun toString() = "rethrown %3d | returned the cached value %3d | returned something else %3d | catch block ran %3d"
        .format(rethrown, returnedCached, returnedOther, catchRan)
}

class Opt(val name: String, val build: (Failure, CoroutineDispatcher, AtomicInteger) -> Flow<String>,
          val neverSuspends: Boolean = false, val callerCatches: Boolean = false)

val io = Dispatchers.IO
val opts = listOf(
    Opt("buffer() after catch", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") }.buffer() }),
    Opt("conflate() after catch", { f, d, c -> source(f).flowOn(d).catch { c.incrementAndGet(); emit("fallback") }.conflate() }),
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

        }
    }
}
