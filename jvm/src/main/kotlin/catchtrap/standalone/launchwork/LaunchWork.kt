package catchtrap.standalone.launchwork

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/*
 * A collector that launches the work on each value, and two ways to limit how many run at once with a
 * Semaphore. Does the error still skip catch?
 *
 * Same setup as the `fixes` check: one fake main thread, flowOn(Dispatchers.IO), catch after it, work of
 * 10 ms. The source emits "a" and "b", then fails 2 ms later from a suspending call. With one permit, the
 * work on "b" can only start once the work on "a" is done.
 *
 * Rows:
 *   collect { work }                                          the trap, to compare
 *   collect { launch { work } }                               the launching collector from `fixes`
 *   collect { launch { permits.withPermit { work } } }        the permit is taken inside the launched work
 *   collect { permits.acquire(); launch { work; release } }   the collector itself waits for the permit
 *
 * Counted per run: the error reached the code that called collect (escaped), catch ran, and the work
 * finished for both values. A value still in flowOn's queue when the source fails can be lost (failure 1),
 * so "work done for both values" can miss a run without any crash.
 */
private class UpstreamError : RuntimeException("upstream failed")

private enum class Row(val label: String) {
    PLAIN("collect { work }"),
    LAUNCH("collect { launch { work } }"),
    PERMIT_INSIDE("collect { launch { permits.withPermit { work } } }"),
    PERMIT_IN_COLLECTOR("collect { permits.acquire(); launch { work; release } }"),
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 500
    val main = Executors.newSingleThreadExecutor { r -> Thread(r, "fake-main").apply { isDaemon = true } }
        .asCoroutineDispatcher()
    println("kotlinx.coroutines 1.11.0, $runs runs per row, collected on one main thread, one permit")
    for (row in Row.entries) {
        var escaped = 0; var catchRan = 0; var bothDone = 0
        repeat(runs) {
            val done = AtomicInteger()
            var ran = false
            val permits = Semaphore(1)
            suspend fun work() { delay(10); done.incrementAndGet() }
            val flow = flow {
                emit("a")
                emit("b")
                delay(2)
                throw UpstreamError()
            }.flowOn(Dispatchers.IO).catch { ran = true }
            try {
                runBlocking(main) {
                    coroutineScope {
                        when (row) {
                            Row.PLAIN -> flow.collect { work() }
                            Row.LAUNCH -> flow.collect { launch { work() } }
                            Row.PERMIT_INSIDE -> flow.collect { launch { permits.withPermit { work() } } }
                            Row.PERMIT_IN_COLLECTOR -> flow.collect {
                                permits.acquire()
                                launch { try { work() } finally { permits.release() } }
                            }
                        }
                    }
                }
            } catch (e: UpstreamError) {
                escaped++
            }
            if (ran) catchRan++
            if (done.get() == 2) bothDone++
        }
        println(
            "   %-58s | escaped %3d/%d | catch ran %3d | work done for both values %3d"
                .format(row.label, escaped, runs, catchRan, bothDone)
        )
    }
    main.close()
}
