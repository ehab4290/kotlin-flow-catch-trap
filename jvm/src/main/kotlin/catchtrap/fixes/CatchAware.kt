/*
 * Contains code copied from kotlinx.coroutines 1.11.0 and changed:
 *  - catchAwareImpl is catchImpl from kotlinx-coroutines-core/common/src/flow/operators/Errors.kt,
 *    with one check added (wasCausedBy).
 *  - isCancellationCause and isSameExceptionAs are copied from the same file.
 *  - unwrap is unwrapImpl from kotlinx-coroutines-core/jvm/src/internal/StackTraceRecovery.kt.
 * kotlinx.coroutines: Copyright 2016-2025 JetBrains s.r.o and contributors, Apache License 2.0.
 */

package catchtrap.fixes

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalForInheritanceCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlin.coroutines.CoroutineContext

/**
 * Like [kotlinx.coroutines.flow.catch], with one extra rule.
 *
 * The library's `catch` rethrows an upstream error whenever the downstream has also thrown. After a
 * channel (`flowOn`, `buffer`, `flatMapLatest`, ...) that includes the case where the collector was
 * merely cancelled BECAUSE the upstream failed. This operator recognizes that case: if the
 * downstream's exception is a cancellation whose cause is the upstream error itself, there is one
 * failure, not two, and the error is handled.
 *
 * What it does not change:
 *  - `first()` / `take()` stopping the flow while the upstream fails: still rethrown.
 *  - a value still in the channel's queue when the upstream fails: still lost.
 *  - the collector's own exceptions: still not caught.
 *
 * Experimental. Tested on the JVM with kotlinx.coroutines 1.11.0 only.
 */
@OptIn(ExperimentalForInheritanceCoroutinesApi::class)
fun <T> Flow<T>.catchAware(action: suspend FlowCollector<T>.(cause: Throwable) -> Unit): Flow<T> {
    val upstream = this
    // Not the public flow { } builder: its safety check forbids emitting after the downstream has thrown.
    return object : Flow<T> {
        override suspend fun collect(collector: FlowCollector<T>) {
            val exception = upstream.catchAwareImpl(collector)
            if (exception != null) collector.action(exception)
        }
    }
}

private suspend fun <T> Flow<T>.catchAwareImpl(collector: FlowCollector<T>): Throwable? {
    var fromDownstream: Throwable? = null
    try {
        collect {
            try {
                collector.emit(it)
            } catch (e: Throwable) {
                fromDownstream = e
                throw e
            }
        }
    } catch (e: Throwable) {
        // From here on, as in catchImpl in kotlinx.coroutines 1.11.0 (Errors.kt), with one check added.
        // Otherwise, smartcast is impossible
        val fromDownstream = fromDownstream
        if (e.isSameExceptionAs(fromDownstream) || e.isCancellationCause(currentCoroutineContext())) {
            throw e // Rethrow exceptions from downstream and cancellation causes
        } else {
            if (fromDownstream == null) {
                return e
            }
            // New: the downstream did not fail on its own. It was cancelled by this upstream error.
            if (fromDownstream is CancellationException &&
                e !is CancellationException &&
                fromDownstream.wasCausedBy(e)
            ) {
                return e
            }
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

// The library's helpers for rules 1 and 2 are private, so these are copies of them.

/** Copy of the private `isCancellationCause` in Errors.kt (1.11.0). */
@OptIn(InternalCoroutinesApi::class) // Job.getCancellationException(), which the library calls here too
private fun Throwable.isCancellationCause(coroutineContext: CoroutineContext): Boolean {
    val job = coroutineContext[Job]
    if (job == null || !job.isCancelled) return false
    return isSameExceptionAs(job.getCancellationException())
}

/** Copy of the private `isSameExceptionAs` in Errors.kt (1.11.0). */
private fun Throwable.isSameExceptionAs(other: Throwable?): Boolean =
    other != null && unwrap(other) == unwrap(this)

/**
 * Copy of the internal `unwrapImpl` in StackTraceRecovery.kt (JVM, 1.11.0). The library calls it only when
 * stack-trace recovery is on; when it is off, no exception carries an artificial frame, so this returns the
 * exception unchanged, as the library does.
 */
private fun <E : Throwable> unwrap(exception: E): E {
    val cause = exception.cause
    // Fast-path to avoid array cloning
    if (cause == null || cause.javaClass != exception.javaClass) {
        return exception
    }
    // Slow path looks for artificial frames in a stack-trace
    if (exception.stackTrace.any { it.className.startsWith("_COROUTINE") }) {
        @Suppress("UNCHECKED_CAST")
        return cause as E
    } else {
        return exception
    }
}

/**
 * True if [error] appears in this exception's cause chain.
 *
 * In debug mode coroutines may hand over a copy of an exception whose `cause` is the original
 * (stack-trace recovery), so the original that [error] wraps counts as well.
 */
private fun Throwable.wasCausedBy(error: Throwable): Boolean {
    val candidates = HashSet<Throwable>()
    var candidate: Throwable? = error
    while (candidate != null && candidates.add(candidate)) candidate = candidate.cause

    val seen = HashSet<Throwable>()
    var cause = this.cause
    while (cause != null && seen.add(cause)) {
        val current = cause
        if (candidates.any { it === current }) return true
        cause = current.cause
    }
    return false
}
