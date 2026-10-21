package catchtrap.fixes

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import catchtrap.flowcatch.newFakeMainDispatcher

/*
 * The ways out of the trap, side by side. Each option runs the same three situations:
 *
 *   collect { delay }   a collector that suspends 10 ms per value; the source fails 2 ms after emitting
 *   first()             the source fails after a blocking call, so cancellation cannot stop it
 *   toList()            the source fails right after emitting, while the value may still be queued
 *
 * ok = "cached" was delivered, lost = it was not, escaped = the error was thrown to the caller.
 */

class UpstreamError : RuntimeException("upstream failed")

private enum class Failure { AFTER_SUSPENDING, AFTER_BLOCKING, IMMEDIATE }

private suspend fun fail(failure: Failure): Nothing {
    when (failure) {
        Failure.AFTER_SUSPENDING -> delay(2)
        Failure.AFTER_BLOCKING -> Thread.sleep(3)
        Failure.IMMEDIATE -> Unit
    }
    throw UpstreamError()
}

private fun source(failure: Failure): Flow<String> = flow {
    emit("cached")
    fail(failure)
}

private class Option(
    val name: String,
    /** The flow the caller receives. */
    val build: (Failure, CoroutineDispatcher) -> Flow<String>,
    /** How the caller collects every value. It records what it was given in `seen`, even if it throws later. */
    val collectAll: suspend (Flow<String>, MutableList<String>) -> Unit = { flow, seen ->
        flow.collect { seen += it; delay(10) }
    },
    /** True when the caller wraps the terminal call in try/catch and treats the error as handled. */
    val callerCatches: Boolean = false,
)

private val options = listOf(
    Option("catch after flowOn (the trap)", { f, io -> source(f).flowOn(io).catch { emit("fallback") } }),
    Option("catch before flowOn", { f, io -> source(f).catch { emit("fallback") }.flowOn(io) }),
    Option(
        "try/catch inside the flow builder",
        { f, io ->
            flow {
                emit("cached")
                try { fail(f) } catch (e: UpstreamError) { emit("fallback") }
            }.flowOn(io)
        },
    ),
    Option(
        "no flowOn: withContext inside the flow",
        { f, io ->
            flow {
                emit(withContext(io) { "cached" })
                withContext(io) { fail(f) }
            }.catch { emit("fallback") }
        },
    ),
    Option("buffer() after catch", { f, io -> source(f).flowOn(io).catch { emit("fallback") }.buffer() }),
    Option(
        "collector never suspends (launches its work)",
        { f, io -> source(f).flowOn(io).catch { emit("fallback") } },
        collectAll = { flow, seen ->
            coroutineScope { flow.collect { seen += it; launch { delay(10) } } }
        },
    ),
    Option(
        "try/catch around the terminal call",
        { f, io -> source(f).flowOn(io).catch { emit("fallback") } },
        callerCatches = true,
    ),
    Option("catchAware instead of catch", { f, io -> source(f).flowOn(io).catchAware { emit("fallback") } }),
)

private data class Tally(var ok: Int = 0, var lost: Int = 0, var escaped: Int = 0) {
    override fun toString(): String = String.format("ok %3d lost %3d escaped %3d", ok, lost, escaped)
}

private suspend fun tally(runs: Int, callerCatches: Boolean, attempt: suspend (seen: MutableList<String>) -> Unit): Tally {
    val tally = Tally()
    repeat(runs) {
        val seen = ArrayList<String>(2)
        try {
            attempt(seen)
            if ("cached" in seen) tally.ok++ else tally.lost++
        } catch (e: UpstreamError) {
            if (!callerCatches) tally.escaped++
            else if ("cached" in seen) tally.ok++ else tally.lost++
        }
    }
    return tally
}

fun runFixOptions(runs: Int, print: (String) -> Unit = ::println) {
    val io = Dispatchers.IO
    newFakeMainDispatcher().use { main ->
        print("")
        print(String.format("%-46s | %-28s | %-28s | %-28s", "option", "collect { delay }", "first()", "toList()"))
        for (option in options) {
            val (collecting, first, list) = runBlocking(main) {
                Triple(
                    tally(runs, option.callerCatches) { seen ->
                        option.collectAll(option.build(Failure.AFTER_SUSPENDING, io), seen)
                    },
                    tally(runs, option.callerCatches) { seen ->
                        seen += option.build(Failure.AFTER_BLOCKING, io).first()
                    },
                    tally(runs, option.callerCatches) { seen ->
                        // Fill `seen` as values arrive, so a later throw does not hide what was delivered.
                        option.build(Failure.IMMEDIATE, io).collect { seen += it }
                    },
                )
            }
            print(String.format("%-46s | %-28s | %-28s | %-28s", option.name, collecting, first, list))
        }
    }
}
