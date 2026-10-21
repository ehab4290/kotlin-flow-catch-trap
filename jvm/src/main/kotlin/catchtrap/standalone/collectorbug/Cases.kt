package catchtrap.standalone.collectorbug

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicInteger

data class Profile(val name: String)
class NetworkError : RuntimeException("network down")
fun lastCachedProfile() = Profile("cached")

// The repository, as in the article: the cached profile, then the network, off the main thread.
fun profileBlocking(): Flow<Profile> = flow {
    emit(Profile("cached"))
    Thread.sleep(5); throw NetworkError()          // blocking call, e.g. Call.execute()
}.flowOn(Dispatchers.IO)

fun profileSuspending(): Flow<Profile> = flow {
    emit(Profile("cached"))
    delay(5); throw NetworkError()                 // suspending call, e.g. a suspend Retrofit function
}.flowOn(Dispatchers.IO)

fun profileFailsRightAway(): Flow<Profile> = flow {
    emit(Profile("cached"))
    throw NetworkError()
}.flowOn(Dispatchers.IO)

fun render(p: Profile): Nothing = error("bug in render()")

class Outcome { val crashed = AtomicInteger(); val networkErrorWithRenderBugSuppressed = AtomicInteger(); val returned = AtomicInteger() }

fun run(label: String, runs: Int = (System.getenv("RUNS")?.toInt() ?: 200), block: suspend () -> Unit, inspect: (Throwable, Outcome) -> Unit = { _, _ -> }) = runBlocking {
    val o = Outcome()
    repeat(runs) {
        var crash: Throwable? = null
        val scope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e -> crash = e })
        scope.launch { block(); o.returned.incrementAndGet() }.join()
        crash?.let { o.crashed.incrementAndGet(); inspect(it, o) }
    }
    println("$label: crashed ${o.crashed}/$runs, completed normally ${o.returned}/$runs" +
        ", network error with render() bug in suppressed ${o.networkErrorWithRenderBugSuppressed}/$runs")
}

fun casesMain() {
    // Case 2, exactly as in the snippet: val profiles = repository.profile().catch { emit(lastCachedProfile()) }; profiles.first()
    run("case 2, blocking call   ", block = { profileBlocking().catch { emit(lastCachedProfile()) }.first() })
    run("case 2, suspending call ", block = { profileSuspending().catch { emit(lastCachedProfile()) }.first() })
    // Case 4: profiles.collect { render(it) }
    run("case 4, render() throws ", block = { profileFailsRightAway().catch { emit(lastCachedProfile()) }.collect { Thread.sleep(5); render(it) } },
        inspect = { e, o -> if (e is NetworkError && e.suppressed.any { it.message == "bug in render()" }) o.networkErrorWithRenderBugSuppressed.incrementAndGet() })
}
