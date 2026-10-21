package catchtrap.standalone.costs

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicInteger

class NetworkError : RuntimeException("network down")
val RUNS = (System.getenv("RUNS")?.toInt() ?: 200)

/** One run in a scope with a handler, like viewModelScope plus a crash counter. */
suspend fun inScope(onCrash: (Throwable) -> Unit, block: suspend CoroutineScope.() -> Unit) {
    val scope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e -> onCrash(e) })
    scope.launch(block = block).join()
}

fun main() = runBlocking {
    // ---------- Bullet 2: which thread runs the catch block when catch sits before flowOn(IO)?
    run {
        val threads = CopyOnWriteArraySet<String>()
        repeat(RUNS) {
            inScope({}) {
                flow { emit("cached"); delay(2); throw NetworkError() }
                    .catch { threads += Thread.currentThread().name.substringBefore("-worker") + "-worker"; emit("fallback") }
                    .flowOn(Dispatchers.IO)
                    .collect { }
            }
        }
        println("B2  catch before flowOn(IO): catch block ran on threads = $threads")
    }

    // ---------- Bullet 3: first() returns the cached value; does the catch block still run (and "log")?
    for ((label, blocking) in listOf("blocking call  " to true, "suspending call" to false)) {
        val gotCached = AtomicInteger(); val logged = AtomicInteger(); val crashed = AtomicInteger()
        repeat(RUNS) {
            inScope({ crashed.incrementAndGet() }) {
                val value = flow {
                    emit("cached")
                    if (blocking) Thread.sleep(5) else delay(5)
                    throw NetworkError()
                }
                    .catch { logged.incrementAndGet(); emit("fallback") } // "logging" + fallback
                    .flowOn(Dispatchers.IO)
                    .first()
                if (value == "cached") gotCached.incrementAndGet()
            }
            delay(20) // time for a late catch block in the producer
        }
        println("B3  first(), $label: returned cached $gotCached/$RUNS, catch block still ran (logged) $logged/$RUNS, crashed $crashed/$RUNS")
    }

    // ---------- Bullet 3b: does first() return before the producer's call has ended?
    // flowOn collects inside coroutineScope { }, which waits for the producer, so the source says no.
    for ((label, blocking) in listOf("blocking call  " to true, "suspending call" to false)) {
        val before = AtomicInteger(); val logged = AtomicInteger(); val crashed = AtomicInteger()
        var minMs = Long.MAX_VALUE; var maxMs = 0L
        repeat(RUNS) {
            inScope({ crashed.incrementAndGet() }) {
                var callEnded = 0L
                val start = System.nanoTime()
                flow {
                    emit("cached")
                    if (blocking) Thread.sleep(50) else delay(50)   // the network call, 50 ms
                    callEnded = System.nanoTime()
                    throw NetworkError()
                }
                    .catch { logged.incrementAndGet(); emit("fallback") }
                    .flowOn(Dispatchers.IO)
                    .first()
                val returned = System.nanoTime()
                if (callEnded == 0L || returned < callEnded) before.incrementAndGet()   // 0: the call was cancelled, never ended
                val ms = (returned - start) / 1_000_000
                minMs = minOf(minMs, ms); maxMs = maxOf(maxMs, ms)
            }
            delay(20)
        }
        println("B3b first(), $label: returned before the call ended $before/$RUNS, catch block ran $logged/$RUNS, crashed $crashed/$RUNS, first() took $minMs to $maxMs ms (the call takes 50 ms)")
    }

    // ---------- After the list, retry: retry(3) before flowOn, with a collector that suspends 10 ms per value
    suspend fun retryRun(label: String, chain: (Flow<String>) -> Flow<String>) {
        val attempts = AtomicInteger(); val threw = AtomicInteger(); val sawFallback = AtomicInteger()
        repeat(RUNS) {
            inScope({ threw.incrementAndGet() }) {
                var last = ""
                chain(flow { attempts.incrementAndGet(); emit("value"); delay(2); throw NetworkError() })
                    .collect { last = it; delay(10) }
                if (last == "fallback") sawFallback.incrementAndGet()
            }
        }
        println("B4  $label: source started ${attempts.get()} times (${4 * RUNS} = 4 per run), threw $threw/$RUNS, ended on fallback $sawFallback/$RUNS")
    }
    retryRun("retry(3).catch { }.flowOn(IO)  ") { it.retry(3).catch { emit("fallback") }.flowOn(Dispatchers.IO) }
    retryRun("retry(3).flowOn(IO), no catch   ") { it.retry(3).flowOn(Dispatchers.IO) }
    retryRun("flowOn(IO).retry(3)  (the trap) ") { it.flowOn(Dispatchers.IO).retry(3) }
}
