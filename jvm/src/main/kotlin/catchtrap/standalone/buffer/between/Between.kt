package catchtrap.standalone.buffer.between

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.util.concurrent.Executors

class Fail : RuntimeException("source failed")

/** catch between flowOn and buffer, with different buffer sizes and different numbers of values before the failure. */
fun main() {
    val main = Executors.newSingleThreadExecutor { Thread(it, "main") }.asCoroutineDispatcher()
    val io = Dispatchers.IO
    val chains = listOf<Pair<String, (Flow<Int>) -> Flow<Int>>>(
        "flowOn(io).catch { }                       " to { f -> f.flowOn(io).catch { emit(-1) } },
        "flowOn(io).catch { }.buffer()       (64)   " to { f -> f.flowOn(io).catch { emit(-1) }.buffer() },
        "flowOn(io).catch { }.buffer(1)             " to { f -> f.flowOn(io).catch { emit(-1) }.buffer(1) },
        "flowOn(io).catch { }.buffer(RENDEZVOUS) (0)" to { f -> f.flowOn(io).catch { emit(-1) }.buffer(Channel.RENDEZVOUS) },
    )
    runBlocking(main) {
        for (values in listOf(1, 2, 3, 100)) {
            val runs = if (values > 10) 100 else 500
            println("source emits $values value(s) quickly, then fails 2 ms later; collector suspends 5 ms per value; $runs runs")
            for ((name, chain) in chains) {
                var rethrown = 0; var fallback = 0
                repeat(runs) {
                    try {
                        chain(flow { repeat(values) { emit(it) }; delay(2); throw Fail() }).collect { if (it == -1) fallback++; delay(5) }
                    } catch (e: Fail) { rethrown++ }
                }
                println("   $name  error skipped catch %3d/%d, fallback reached the collector %3d/%d".format(rethrown, runs, fallback, runs))
            }
        }
    }
    main.close()
}
