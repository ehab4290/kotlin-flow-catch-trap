package catchtrap.standalone.placement

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/* No flowOn. Where does an added flatMapLatest / combine / buffer bring the trap back: before catch, or also after it? */

class NetErr : RuntimeException("network down")

fun main() = runBlocking {
    val runs = (System.getenv("RUNS")?.toInt() ?: 200)
    val main = Executors.newSingleThreadExecutor { Thread(it, "main") }.asCoroutineDispatcher()
    val ops = listOf<Pair<String, (Flow<String>) -> Flow<String>>>(
        "flatMapLatest { flowOf(it) }" to { f -> f.flatMapLatest { flowOf(it) } },
        "combine(flowOf(1)) { a, _ -> a }" to { f -> f.combine(flowOf(1)) { a, _ -> a } },
        "buffer()" to { f -> f.buffer() },
    )
    for ((name, op) in ops) for (before in listOf(true, false)) {
        val crashed = AtomicInteger(); val handled = AtomicInteger(); val fallbackSeen = AtomicInteger()
        repeat(runs) {
            val scope = CoroutineScope(SupervisorJob() + main + CoroutineExceptionHandler { _, _ -> crashed.incrementAndGet() })
            scope.launch {
                val src = flow { emit("cached"); delay(5); throw NetErr() }
                val withCatch: (Flow<String>) -> Flow<String> = { it.catch { handled.incrementAndGet(); emit("fallback") } }
                val chain = if (before) withCatch(op(src)) else op(withCatch(src))
                chain.collect { if (it == "fallback") fallbackSeen.incrementAndGet(); delay(20) }
            }.join()
        }
        println("%-34s %-16s crashed %3d/%d  catch ran %3d  fallback reached the collector %3d"
            .format(name, if (before) "before catch" else "after catch", crashed.get(), runs, handled.get(), fallbackSeen.get()))
    }
    main.close()
}
