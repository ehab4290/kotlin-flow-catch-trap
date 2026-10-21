package catchtrap.standalone.buffer.ahead

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/** How far can the source run ahead of a slow collector? = values produced but not yet collected. */
fun main() {
    val main = Executors.newSingleThreadExecutor { Thread(it, "main") }.asCoroutineDispatcher()
    for ((name, build) in listOf<Pair<String, (Flow<Int>) -> Flow<Int>>>(
        "no channel: .catch { }" to { f -> f.catch { } },
        "flowOn(io).catch { }" to { f -> f.flowOn(Dispatchers.IO).catch { } },
        "flowOn(io).buffer().catch { }  (fused)" to { f -> f.flowOn(Dispatchers.IO).buffer().catch { } },
        "flowOn(io).catch { }.buffer()  (two channels)" to { f -> f.flowOn(Dispatchers.IO).catch { }.buffer() },
    )) {
        val produced = AtomicInteger(); var collected = 0; var maxAhead = 0
        runBlocking(main) {
            build(flow { repeat(400) { produced.incrementAndGet(); emit(it) } })
                .collect { delay(2); collected++; maxAhead = maxOf(maxAhead, produced.get() - collected) }
        }
        println("%-48s source ran at most %3d values ahead of the collector".format(name, maxAhead))
    }
    main.close()
}
