package catchtrap.standalone.retry

import catchtrap.flowcatch.newFakeMainDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger

/*
 * flowOn(IO).retry(3) with a suspending collector: does retry rethrow without retrying? Counts, per run,
 * whether the error reached the caller and how many times the source was started.
 *
 * Same setup as section 4 of the `checks` report, counted per run: the source emits, fails 2 ms later,
 * and the collector suspends 10 ms per value. Default 10,000 runs; RUNS=… to change.
 */
class UpstreamError : RuntimeException("upstream failed")

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 10_000
    var threw = 0
    var startedOnce = 0
    var startedMore = 0
    newFakeMainDispatcher().use { main ->
        runBlocking(main) {
            repeat(runs) {
                val attempts = AtomicInteger()
                try {
                    flow {
                        attempts.incrementAndGet()
                        emit("cached")
                        delay(2)
                        throw UpstreamError()
                    }.flowOn(Dispatchers.IO).retry(3).collect { delay(10) }
                } catch (e: UpstreamError) {
                    threw++
                }
                if (attempts.get() == 1) startedOnce++ else startedMore++
            }
        }
    }
    println("kotlinx.coroutines 1.11.0, $runs runs")
    println("flowOn(IO).retry(3), suspending collector: threw $threw / $runs, source started once in $startedOnce / $runs, more than once in $startedMore / $runs")
}
