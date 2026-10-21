package catchtrap.fixes

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import catchtrap.flowcatch.newFakeMainDispatcher

/*
 * What catchAware fixes, what it leaves alone, and what it costs. Then what a
 * CoroutineExceptionHandler does instead.
 *
 * Unless a row says otherwise, the source emits "cached" and fails 2 ms later, and the collector
 * suspends 10 ms per value, so it is always suspended when the source fails.
 */

private class CollectorBug : RuntimeException("the collector's own bug")

private fun failingSource(failAfterMs: Long = 2, blocking: Boolean = false): Flow<String> = flow {
    emit("cached")
    if (blocking) Thread.sleep(failAfterMs) else delay(failAfterMs)
    throw UpstreamError()
}

private class Named<T>(val name: String, val value: T)

@OptIn(ExperimentalCoroutinesApi::class)
fun runCatchAwareChecks(runs: Int, print: (String) -> Unit = ::println) {
    val io = Dispatchers.IO
    newFakeMainDispatcher().use { main ->

        // 1. Different channels before catchAware.
        val channels = listOf(
            Named<() -> Flow<String>>("flowOn(io)") { failingSource().flowOn(io) },
            Named<() -> Flow<String>>("buffer(), everything on one thread") { failingSource().buffer() },
            Named<() -> Flow<String>>("flatMapLatest { source }") { flowOf(Unit).flatMapLatest { failingSource() } },
        )
        print("")
        print("a channel, then catchAware { emit(\"fallback\") }, then a collector that suspends")
        print(String.format("%-36s | %-18s | %s", "before catchAware", "fallback delivered", "error rethrown"))
        for (channel in channels) {
            var fallbackDelivered = 0
            var rethrown = 0
            runBlocking(main) {
                repeat(runs) {
                    val seen = ArrayList<String>(2)
                    try {
                        channel.value().catchAware { emit("fallback") }.collect { seen += it; delay(10) }
                    } catch (e: UpstreamError) {
                        rethrown++
                    }
                    if ("fallback" in seen) fallbackDelivered++
                }
            }
            print(String.format("%-36s | %-18d | %d", channel.name, fallbackDelivered, rethrown))
        }

        // 2. Another operator between catchAware and the collector.
        val operators = listOf(
            Named<(Flow<String>) -> Flow<String>>("nothing") { it },
            Named<(Flow<String>) -> Flow<String>>("map { it }") { flow -> flow.map { it } },
            Named<(Flow<String>) -> Flow<String>>("onEach { }") { flow -> flow.onEach { } },
            Named<(Flow<String>) -> Flow<String>>("transform { emit(it) }") { flow -> flow.transform { emit(it) } },
            Named<(Flow<String>) -> Flow<String>>("flow { collect { emit(it) } }") { upstream -> flow { upstream.collect { emit(it) } } },
        )
        print("")
        print("flowOn(io).catchAware { emit(\"fallback\") }, then another operator, then a collector that suspends")
        print(String.format("%-36s | %-18s | %-21s | %s", "after catchAware", "fallback delivered", "IllegalStateException", "work on \"cached\" interrupted"))
        for (operator in operators) {
            var fallbackDelivered = 0
            var transparencyViolated = 0
            var interrupted = 0
            runBlocking(main) {
                repeat(runs) {
                    val seen = ArrayList<String>(2)
                    try {
                        operator.value(failingSource().flowOn(io).catchAware { emit("fallback") }).collect { value ->
                            seen += value
                            try {
                                delay(10)
                            } catch (e: CancellationException) {
                                if (value == "cached") interrupted++
                                throw e
                            }
                        }
                    } catch (e: IllegalStateException) {
                        transparencyViolated++
                    }
                    if ("fallback" in seen) fallbackDelivered++
                }
            }
            print(String.format("%-36s | %-18d | %-21d | %d", operator.name, fallbackDelivered, transparencyViolated, interrupted))
        }

        // 3. The cases catchAware must not, or does not, change.
        var collectorThrew = 0
        var bugReachedCaller = 0
        runBlocking(main) {
            repeat(runs) {
                try {
                    failingSource().flowOn(io).catchAware { }.collect { collectorThrew++; throw CollectorBug() }
                } catch (e: CollectorBug) {
                    bugReachedCaller++
                }
            }
        }
        print("")
        print("flowOn(io).catchAware { }, other situations")
        print("the collector throws its own exception: thrown in $collectorThrew runs, reached the caller in $bugReachedCaller")

        // The user leaves 2 ms in. The source fails 20 ms in, long after the scope is gone.
        for (blocking in listOf(false, true)) {
            var uncaught = 0
            var blockRan = 0
            runBlocking {
                repeat(runs) {
                    val scope = CoroutineScope(SupervisorJob() + main)
                    val handler = CoroutineExceptionHandler { _, _ -> uncaught++ }
                    val job = scope.launch(handler) {
                        failingSource(failAfterMs = 20, blocking = blocking).flowOn(io)
                            .catchAware { blockRan++ }
                            .collect { delay(50) }
                    }
                    delay(2)
                    scope.cancel()
                    job.join()
                }
            }
            val call = if (blocking) "blocking call" else "suspending call"
            print("the scope is cancelled before the source fails, $call: uncaught $uncaught, catchAware block ran $blockRan")
        }

        // 4. The library's catch after flowOn, launched with a CoroutineExceptionHandler.
        var handlerCalls = 0
        var catchBlockRuns = 0
        var fallbackDelivered = 0
        runBlocking {
            repeat(runs) {
                val scope = CoroutineScope(SupervisorJob() + main)
                val handler = CoroutineExceptionHandler { _, error -> if (error is UpstreamError) handlerCalls++ }
                scope.launch(handler) {
                    failingSource().flowOn(io)
                        .catch { catchBlockRuns++; emit("fallback") }
                        .collect { value ->
                            if (value == "fallback") fallbackDelivered++
                            delay(10)
                        }
                }.join()
            }
        }
        print("")
        print("catch after flowOn(io), launched with a CoroutineExceptionHandler")
        print("handler received the error $handlerCalls, catch block ran $catchBlockRuns, fallback delivered $fallbackDelivered")
    }
}
