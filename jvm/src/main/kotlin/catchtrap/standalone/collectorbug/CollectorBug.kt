package catchtrap.standalone.collectorbug

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*

/** The cached profile, then a network call of [callMs] that fails: blocking (Thread.sleep) or suspending (delay). */
fun profileFailingAfter(blocking: Boolean, callMs: Long): Flow<Profile> = flow {
    emit(Profile("cached"))
    if (blocking) Thread.sleep(callMs) else delay(callMs)
    throw NetworkError()
}.flowOn(Dispatchers.IO)

fun main() {
    val bugHidden: (Throwable, Outcome) -> Unit = { e, o ->
        if (e is NetworkError && e.suppressed.any { it.message == "bug in render()" }) o.networkErrorWithRenderBugSuppressed.incrementAndGet()
    }
    repeat(5) { i ->
        run("case 4 batch ${i + 1}", block = { profileFailsRightAway().catch { emit(lastCachedProfile()) }.collect { Thread.sleep(5); render(it) } },
            inspect = bugHidden)
    }
    // Which side fails first? render() throws 5 ms after the cached profile arrives. The network call fails
    // after 20 ms (the bug comes first, during the call) or after 5 ms (about the same time).
    // Every run crashes; the ones not counted as "network error with render() bug in suppressed" got the bug itself.
    for ((callMs, timing) in listOf(20L to "bug during the call", 5L to "about the same time")) {
        for (blocking in listOf(true, false)) {
            val call = if (blocking) "blocking call  " else "suspending call"
            run("case 4 order: $call $callMs ms, render() throws after 5 ms ($timing)",
                block = { profileFailingAfter(blocking, callMs).catch { emit(lastCachedProfile()) }.collect { Thread.sleep(5); render(it) } },
                inspect = bugHidden)
        }
    }
}
