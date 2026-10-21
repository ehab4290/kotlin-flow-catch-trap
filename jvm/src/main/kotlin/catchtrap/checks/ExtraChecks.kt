package catchtrap.checks

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.all
import kotlinx.coroutines.flow.any
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.none
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import catchtrap.flowcatch.newFakeMainDispatcher
import java.util.concurrent.atomic.AtomicInteger

/*
 * The smaller claims: early-stop operators, a collector that throws, a collector that never
 * suspends, retry on either side of flowOn, a plain try/catch, and shareIn / stateIn.
 * Everything is collected on a single main thread.
 */

class UpstreamError : RuntimeException("upstream failed")
class CollectorBug : RuntimeException("bug in the collector")

private fun failsImmediately(attempts: AtomicInteger? = null): Flow<String> = flow {
    attempts?.incrementAndGet()
    emit("cached")
    throw UpstreamError()
}

private fun failsAfterSuspending(attempts: AtomicInteger? = null): Flow<String> = flow {
    attempts?.incrementAndGet()
    emit("cached")
    delay(2)
    throw UpstreamError()
}

private fun failsAfterBlocking(): Flow<String> = flow {
    emit("cached")
    Thread.sleep(2)
    throw UpstreamError()
}

fun runExtraChecks(runs: Int, print: (String) -> Unit = ::println) {
    newFakeMainDispatcher().use { main ->
        runBlocking(main) {
            print("")
            print("== 1. Operators that stop the flow early, after flowOn(IO), source fails right after emitting")
            val earlyStops: List<Pair<String, suspend (Flow<String>) -> Unit>> = listOf(
                "first()" to { f -> f.first() },
                "firstOrNull()" to { f -> f.firstOrNull() },
                "take(1).toList()" to { f -> f.take(1).toList() },
                "takeWhile { false }.toList()" to { f -> f.takeWhile { false }.toList() },
                "any { true }" to { f -> f.any { true } },
                "all { false }" to { f -> f.all { false } },
                "none { true }" to { f -> f.none { true } },
                "first { true }" to { f -> f.first { true } },
                "toList()  (control: never stops early)" to { f -> f.toList() },
            )
            for ((name, terminal) in earlyStops) {
                var escaped = 0
                repeat(runs) {
                    try {
                        terminal(failsImmediately().flowOn(Dispatchers.IO).catch { emit("fallback") })
                    } catch (e: UpstreamError) {
                        escaped++
                    }
                }
                print(String.format("%-42s upstream error thrown past catch: %3d / %d", name, escaped, runs))
            }

            print("")
            print("== 2. The collector throws its own exception, after flowOn(IO)")
            var sawOwnBug = 0
            var sawUpstream = 0
            repeat(runs) {
                try {
                    failsImmediately().flowOn(Dispatchers.IO).catch { emit("fallback") }.collect { throw CollectorBug() }
                } catch (e: CollectorBug) {
                    sawOwnBug++
                } catch (e: UpstreamError) {
                    sawUpstream++
                }
            }
            print("caller saw the collector's own exception: $sawOwnBug, saw the upstream error instead: $sawUpstream (of $runs)")

            print("")
            print("== 3. A collector that never suspends, after flowOn(IO)")
            val styles: List<Pair<String, () -> Flow<String>>> = listOf(
                "source fails immediately" to { failsImmediately() },
                "source fails after a suspending call" to { failsAfterSuspending() },
                "source fails after a blocking call" to { failsAfterBlocking() },
            )
            for ((name, source) in styles) {
                var escaped = 0
                var lost = 0
                repeat(runs) {
                    val seen = ArrayList<String>(2)
                    try {
                        source().flowOn(Dispatchers.IO).catch { emit("fallback") }.collect { seen += it }
                    } catch (e: UpstreamError) {
                        escaped++
                    }
                    if ("cached" !in seen) lost++
                }
                print(String.format("%-42s error thrown past catch: %3d / %d, cached value lost: %3d / %d", name, escaped, runs, lost, runs))
            }

            print("")
            print("== 4. retry(3) with a collector that suspends 10 ms per value, source fails 2 ms after emitting")
            run {
                val attempts = AtomicInteger()
                var threw = 0
                repeat(runs) {
                    try {
                        failsAfterSuspending(attempts).flowOn(Dispatchers.IO).retry(3).collect { delay(10) }
                    } catch (e: UpstreamError) {
                        threw++
                    }
                }
                print("retry AFTER flowOn:   threw $threw / $runs, source started ${attempts.get()} times (${4 * runs} if every run made 4 attempts)")
            }
            run {
                val attempts = AtomicInteger()
                var threw = 0
                var sawFallback = 0
                repeat(runs) {
                    val seen = ArrayList<String>(5)
                    try {
                        failsAfterSuspending(attempts).retry(3).catch { emit("fallback") }.flowOn(Dispatchers.IO)
                            .collect { seen += it; delay(10) }
                    } catch (e: UpstreamError) {
                        threw++
                    }
                    if (seen.lastOrNull() == "fallback") sawFallback++
                }
                print("retry BEFORE flowOn:  threw $threw / $runs, source started ${attempts.get()} times (${4 * runs} if every run made 4 attempts), collector ended on the fallback $sawFallback / $runs")
            }

            print("")
            print("== 5. A plain try/catch around collect, catch after flowOn(IO), suspending collector")
            run {
                val reachedHandler = AtomicInteger()
                var caught = 0
                repeat(runs) {
                    val handler = CoroutineExceptionHandler { _, _ -> reachedHandler.incrementAndGet() }
                    val scope = CoroutineScope(SupervisorJob() + main + handler)
                    scope.launch {
                        try {
                            failsAfterSuspending().flowOn(Dispatchers.IO).catch { emit("fallback") }.collect { delay(10) }
                        } catch (e: UpstreamError) {
                            caught++
                        }
                    }.join()
                }
                print("try/catch caught the error: $caught / $runs, error reached the scope's handler: ${reachedHandler.get()} / $runs")
            }

            print("")
            print("== 6. shareIn / stateIn: where does the upstream error go?")
            for (kind in listOf("shareIn(scope, Eagerly, replay = 1)", "stateIn(scope, Eagerly, \"initial\")")) {
                val reachedScope = AtomicInteger()
                var catchRan = 0
                var thrownToCollector = 0
                repeat(runs) {
                    val handler = CoroutineExceptionHandler { _, e -> if (e is UpstreamError) reachedScope.incrementAndGet() }
                    val scope = CoroutineScope(SupervisorJob() + main + handler)
                    val shared: Flow<String> =
                        if (kind.startsWith("shareIn")) failsAfterSuspending().shareIn(scope, SharingStarted.Eagerly, replay = 1)
                        else failsAfterSuspending().stateIn(scope, SharingStarted.Eagerly, "initial")
                    val seen = ArrayList<String>(3)
                    try {
                        withTimeoutOrNull(30) { collectWithCatch(shared, seen) }
                    } catch (e: UpstreamError) {
                        thrownToCollector++
                    }
                    if ("fallback" in seen) catchRan++
                    scope.cancel()
                }
                print(String.format("%-38s error reached the sharing scope's handler: %3d / %d, catch after it ran: %d, thrown to the collector: %d", kind, reachedScope.get(), runs, catchRan, thrownToCollector))
            }
        }
    }
}

private suspend fun collectWithCatch(flow: Flow<String>, seen: MutableList<String>) {
    val collector = FlowCollector<String> { seen += it }
    flow.catch { emit("fallback") }.collect(collector)
}
