/*
 * Contains code copied from kotlinx.coroutines 1.11.0 and changed:
 *  - tracedCatch and tracedCatchImpl are catch and catchImpl from
 *    kotlinx-coroutines-core/common/src/flow/operators/Errors.kt, with logging added and the
 *    differences listed below.
 *  - isCancellationCause is copied from the same file; isSameExceptionAs is simplified (see below).
 * kotlinx.coroutines: Copyright 2016-2025 JetBrains s.r.o and contributors, Apache License 2.0.
 */

@file:OptIn(InternalCoroutinesApi::class)

package catchtrap.trace

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalForInheritanceCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import catchtrap.flowcatch.newFakeMainDispatcher
import java.util.Collections
import kotlin.coroutines.CoroutineContext

/*
 * A copy of kotlinx.coroutines' catch / catchImpl (flow/operators/Errors.kt, 1.11.0) that logs
 * every decision. Two deliberate differences from the library:
 *  - the library compares exceptions with an internal unwrap() (stack-trace recovery); this copy uses ==.
 *  - the library builds the flow with its internal unsafeFlow; this copy implements Flow directly,
 *    which is equivalent (no extra cancellation check on emit, unlike the public flow { } builder).
 */

class NetworkError : RuntimeException("upstream failed")

private val trace: MutableList<String> = Collections.synchronizedList(mutableListOf())

private fun log(message: String) {
    val thread = Thread.currentThread().name.substringBefore("-worker")
    trace += String.format("%-19s %s", "[$thread]", message)
}

private fun Throwable?.simpleName(): String = this?.javaClass?.simpleName ?: "nothing"

@OptIn(ExperimentalForInheritanceCoroutinesApi::class)
fun <T> Flow<T>.tracedCatch(action: suspend FlowCollector<T>.(Throwable) -> Unit): Flow<T> {
    val upstream = this
    return object : Flow<T> {
        override suspend fun collect(collector: FlowCollector<T>) {
            val exception = upstream.tracedCatchImpl(collector)
            if (exception != null) {
                log("catch: calling your catch { } block with ${exception.simpleName()}")
                collector.action(exception)
            }
        }
    }
}

private suspend fun <T> Flow<T>.tracedCatchImpl(collector: FlowCollector<T>): Throwable? {
    var fromDownstream: Throwable? = null
    try {
        collect {
            try {
                log("catch: passing \"$it\" downstream")
                collector.emit(it)
            } catch (e: Throwable) {
                fromDownstream = e
                log("catch: downstream threw ${e.simpleName()} -> written in notebook")
                throw e
            }
        }
    } catch (e: Throwable) {
        val fromDownstream = fromDownstream
        log("catch: collection ended with e = ${e.simpleName()}, notebook = ${fromDownstream.simpleName()}")
        if (e.isSameExceptionAs(fromDownstream) || e.isCancellationCause(currentCoroutineContext())) {
            log("catch: e is the downstream's own exception (or a cancellation) -> rethrow it untouched")
            throw e
        } else {
            if (fromDownstream == null) {
                log("catch: notebook empty -> pure upstream error -> HANDLE IT")
                return e
            }
            log("catch: notebook NOT empty -> \"both failed\" -> rethrow ${e.simpleName()}")
            if (e is CancellationException) {
                fromDownstream.addSuppressed(e)
                throw fromDownstream
            } else {
                e.addSuppressed(fromDownstream)
                throw e
            }
        }
    }
    return null
}

private fun Throwable.isCancellationCause(coroutineContext: CoroutineContext): Boolean {
    val job = coroutineContext[Job]
    if (job == null || !job.isCancelled) return false
    return isSameExceptionAs(job.getCancellationException())
}

private fun Throwable.isSameExceptionAs(other: Throwable?): Boolean = other != null && other == this

/** Emits, then a suspending network call fails 5 ms later. */
private fun slowSource(): Flow<String> = flow {
    log("producer: emit(\"cached\")")
    emit("cached")
    log("producer: emit returned, network call suspends 5 ms")
    delay(5)
    log("producer: network call throws NetworkError")
    throw NetworkError()
}

/** Emits, then fails with no suspension point in between. */
private fun fastSource(): Flow<String> = flow {
    log("producer: emit(\"cached\")")
    emit("cached")
    log("producer: emit returned -> now throwing NetworkError")
    throw NetworkError()
}

private suspend fun save(value: String) {
    log("collector: got \"$value\", save() suspends 20 ms")
    try {
        delay(20)
    } catch (e: Throwable) {
        log("collector: save() was cancelled: ${e.simpleName()} (${e.message})")
        throw e
    }
    log("collector: save() done")
}

private fun traced(title: String, print: (String) -> Unit, block: suspend () -> Unit) {
    newFakeMainDispatcher("main").use { main ->
        trace.clear()
        runBlocking(main) {
            try {
                block()
                log("RESULT: completed normally")
            } catch (e: NetworkError) {
                log("RESULT: NetworkError thrown to the caller (suppressed: ${e.suppressed.joinToString { it.simpleName() }})")
            }
        }
        Thread.sleep(50) // let late producer lines arrive
    }
    print("")
    print("==== $title")
    synchronized(trace) { trace.forEach(print) }
}

/** Prints four traces: the bug twice (two threads, one thread), then the fix twice. */
fun runTraces(print: (String) -> Unit = ::println) {
    traced("BUG  source.flowOn(IO).catch { }.collect { save(it) }", print) {
        slowSource().flowOn(Dispatchers.IO)
            .tracedCatch { log("catch block: emit(\"error state\")"); emit("error state") }
            .collect { save(it) }
    }
    traced("BUG  source.buffer().catch { }.collect { save(it) }   (everything on the main thread)", print) {
        slowSource().buffer()
            .tracedCatch { log("catch block: emit(\"error state\")"); emit("error state") }
            .collect { save(it) }
    }
    traced("FIX  source.catch { }.flowOn(IO).collect { save(it) }", print) {
        slowSource()
            .tracedCatch { log("catch block: emit(\"error state\")"); emit("error state") }
            .flowOn(Dispatchers.IO)
            .collect { save(it) }
    }
    traced("FIX  source.catch { }.flowOn(IO).first()", print) {
        val value = fastSource()
            .tracedCatch { log("catch block: emit(\"fallback\")"); emit("fallback") }
            .flowOn(Dispatchers.IO)
            .onEach { log("first(): received \"$it\" -> stops the flow by throwing AbortFlowException") }
            .first()
        log("first() returned \"$value\"")
    }
}
