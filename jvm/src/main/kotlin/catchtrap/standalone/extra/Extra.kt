package catchtrap.standalone.extra

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

val RUNS = (System.getenv("RUNS")?.toInt() ?: 200)
class NetworkError : IOException("network down")

/** A single "main" thread, like Dispatchers.Main.immediate in viewModelScope. */
val mainThread = Executors.newSingleThreadExecutor { Thread(it, "main") }.asCoroutineDispatcher()

class Tally { val crashed = AtomicInteger(); val handled = AtomicInteger(); val other = AtomicInteger() }

/** One run in a scope that has no handler of its own; the crash counter plays the default handler. */
suspend fun oneRun(t: Tally, block: suspend CoroutineScope.() -> Unit) {
    val scope = CoroutineScope(SupervisorJob() + mainThread + CoroutineExceptionHandler { _, _ -> t.crashed.incrementAndGet() })
    scope.launch(block = block).join()
}

fun report(label: String, t: Tally, extra: String = "") =
    println("%-62s crashed %3d/%d  catch handled %3d/%d %s".format(label, t.crashed.get(), RUNS, t.handled.get(), RUNS, extra))

fun main() = runBlocking {
    // T3: a try around emit() also catches the collector's exception
    run {
        var caughtCollectorBug = 0
        repeat(RUNS) {
            try {
                flow {
                    try { emit(1) } catch (e: IllegalStateException) { caughtCollectorBug++ ; throw e }
                }.collect { throw IllegalStateException("bug in the collector") }
            } catch (_: IllegalStateException) {}
        }
        println("T3  try around emit(): caught the collector's own exception $caughtCollectorBug/$RUNS")
    }

    // T4: try/catch around the call only: the flow keeps going after a failure
    run {
        var keptGoing = 0
        repeat(RUNS) {
            val values = flow {
                emit("cached")
                val fresh = try { throw NetworkError() } catch (e: IOException) { "failure" }
                emit(fresh)
                emit("next refresh")  // the flow keeps going
            }.toList()
            if (values == listOf("cached", "failure", "next refresh")) keptGoing++
        }
        println("T4  try/catch around the call only: flow kept going after the failure $keptGoing/$RUNS")
    }

    // T6 / T7: no channel. Everything main-safe (suspending), no flowOn; and a blocking call in withContext(io)
    for ((label, chain) in listOf<Pair<String, () -> Flow<String>>>(
        "T6  no flowOn, suspending calls, collector suspends" to {
            flow { emit("cached"); delay(5); throw NetworkError() }
        },
        "T7  withContext(io) around the blocking call only" to {
            flow { emit("cached"); withContext(Dispatchers.IO) { Thread.sleep(5); throw NetworkError() } }
        },
    )) {
        val t = Tally()
        repeat(RUNS) { oneRun(t) { chain().catch { t.handled.incrementAndGet(); emit("fallback") }.collect { delay(20) } } }
        report(label, t)
    }
    // T7b: emit() inside withContext is refused
    run {
        var refused = 0
        repeat(RUNS) {
            try { flow<String> { withContext(Dispatchers.IO) { emit("x") } }.collect { } }
            catch (e: IllegalStateException) { if (e.message?.contains("Flow invariant is violated") == true) refused++ }
        }
        println("T7b emit() inside withContext: refused with 'Flow invariant is violated' $refused/$RUNS")
    }

    // T8: the trap comes back when someone adds flatMapLatest, combine or buffer further down the chain
    for ((label, op) in listOf<Pair<String, (Flow<String>) -> Flow<String>>>(
        "T8  no flowOn, then .flatMapLatest { flowOf(it) }" to { f -> f.flatMapLatest { flowOf(it) } },
        "T8  no flowOn, then .combine(flowOf(1)) { a, _ -> a }" to { f -> f.combine(flowOf(1)) { a, _ -> a } },
        "T8  no flowOn, then .buffer()" to { f -> f.buffer() },
    )) {
        val t = Tally()
        repeat(RUNS) { oneRun(t) { op(flow { emit("cached"); delay(5); throw NetworkError() }).catch { t.handled.incrementAndGet(); emit("fallback") }.collect { delay(20) } } }
        report(label, t)
    }

    // T9: buffer() after catch: until its 64 values are full, the collector no longer slows the producer
    run {
        var produced = 0
        var maxAhead = 0
        var collected = 0
        flow { repeat(200) { produced++; emit(it) } }
            .buffer()
            .collect { delay(1); collected++; maxAhead = maxOf(maxAhead, produced - collected) }
        println("T9  buffer() default: producer ran at most $maxAhead values ahead of the collector")
    }

    // T11: a CoroutineExceptionHandler: the app survives, but does the catch block run?
    run {
        val survived = AtomicInteger(); val blockRan = AtomicInteger()
        repeat(RUNS) {
            val scope = CoroutineScope(SupervisorJob() + mainThread + CoroutineExceptionHandler { _, _ -> survived.incrementAndGet() })
            scope.launch {
                flow { emit("cached"); delay(5); throw NetworkError() }.flowOn(Dispatchers.IO)
                    .catch { blockRan.incrementAndGet(); emit("fallback") }
                    .collect { delay(20) }
            }.join()
        }
        println("T11 handler installed, catch after flowOn: exception reached the handler (app survives) $survived/$RUNS, catch block ran $blockRan/$RUNS")
    }

    // P3: snackbar driven by state. The flow's collector only writes state; the waiting happens in another coroutine.
    run {
        val t = Tally(); var fallbackShown = 0
        repeat(RUNS) {
            val uiState = MutableStateFlow("loading")
            val scope = CoroutineScope(SupervisorJob() + mainThread + CoroutineExceptionHandler { _, _ -> t.crashed.incrementAndGet() })
            val shown = mutableListOf<String>()
            val ui = scope.launch { uiState.collect { msg -> shown += msg; delay(30) } } // LaunchedEffect(state.message) { showSnackbar(...) }
            scope.launch {
                flow { emit("cached"); delay(5); throw NetworkError() }.flowOn(Dispatchers.IO)
                    .catch { t.handled.incrementAndGet(); emit("fallback") }
                    .collect { uiState.value = it }
            }.join()
            delay(80); ui.cancel()
            if ("fallback" in shown) fallbackShown++
        }
        report("P3  state-driven snackbar (collect { _uiState.value = it })", t, "fallback shown $fallbackShown/$RUNS")
    }

    // P6: Channel(BUFFERED) makes send() wait only once 64 messages are pending
    run {
        val ch = Channel<Int>(Channel.BUFFERED)
        var sentWithoutWaiting = 0
        while (ch.trySend(sentWithoutWaiting).isSuccess) sentWithoutWaiting++
        println("P6  Channel(BUFFERED): send() did not wait for the first $sentWithoutWaiting messages")
    }

    mainThread.close()
}
