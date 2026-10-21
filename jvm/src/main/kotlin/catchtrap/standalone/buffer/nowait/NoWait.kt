package catchtrap.standalone.buffer.nowait

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.util.concurrent.Executors

class Fail : RuntimeException("source failed")

/** A second channel after catch whose send never waits: does the error still skip catch when many values are pending? */
fun main() {
    val main = Executors.newSingleThreadExecutor { Thread(it, "main") }.asCoroutineDispatcher()
    val io = Dispatchers.IO
    val chains = listOf<Pair<String, (Flow<Int>) -> Flow<Int>>>(
        ".catch { }.buffer()                         (64, waits when full)" to { f -> f.flowOn(io).catch { emit(-1) }.buffer() },
        ".catch { }.buffer(onBufferOverflow = DROP_OLDEST)" to { f -> f.flowOn(io).catch { emit(-1) }.buffer(onBufferOverflow = BufferOverflow.DROP_OLDEST) },
        ".catch { }.conflate()" to { f -> f.flowOn(io).catch { emit(-1) }.conflate() },
        ".catch { }.buffer(Channel.UNLIMITED)" to { f -> f.flowOn(io).catch { emit(-1) }.buffer(Channel.UNLIMITED) },
    )
    runBlocking(main) {
        for (values in listOf(1, 3, 100)) {
            val runs = if (values > 10) 100 else 500
            println("source emits $values value(s), then fails 2 ms later; collector suspends 5 ms per value; $runs runs")
            for ((name, chain) in chains) {
                var leaked = 0; var fallback = 0; var shown = 0
                repeat(runs) {
                    try {
                        chain(flow { repeat(values) { emit(it) }; delay(2); throw Fail() }).collect { if (it == -1) fallback++ else shown++; delay(5) }
                    } catch (e: Fail) { leaked++ }
                }
                println("   %-66s leaked %3d/%d | fallback shown %3d/%d | values shown per run %.1f".format(name, leaked, runs, fallback, runs, shown.toDouble() / runs))
            }
        }
    }
    main.close()
}
