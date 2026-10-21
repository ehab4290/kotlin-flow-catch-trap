package catchtrap.standalone.sharein

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

// The ViewModel hands updates to the UI with shareIn. catch is after flowOn, before shareIn.
// The source emits n values quickly, then fails. The UI subscriber is busy (a snackbar) on every value.
// The source fails only once the sharing coroutine has received all n values (or after 500 ms), so a
// value still in flowOn's own queue can't decide the row: what's measured is room in shareIn's buffer.
class Outcome(val crashed: Boolean, val catchRan: Boolean, val received: Int)

fun run(n: Int, replay: Int, subscriber: Boolean): Outcome = runBlocking {
    val crashed = AtomicBoolean(false)
    val catchRan = AtomicBoolean(false)
    val received = AtomicInteger()
    val vmScope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, _ -> crashed.set(true) })
    val uiScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    val source = flow {
        repeat(n) { emit(it) }
        withTimeoutOrNull(500) { while (received.get() < n) delay(1) }   // until the sharing coroutine has them all
        throw IOException("network down")
    }
    val shared = source.flowOn(Dispatchers.IO)
        .catch { catchRan.set(true); emit(-1) }
        .onEach { if (it >= 0) received.incrementAndGet() }               // what the sharing coroutine received
        .shareIn(vmScope, if (subscriber) SharingStarted.Lazily else SharingStarted.Eagerly, replay)
    if (subscriber) uiScope.launch { shared.collect { delay(10_000) } }   // showSnackbar()
    val deadline = System.currentTimeMillis() + 2_500
    while (!crashed.get() && !catchRan.get() && System.currentTimeMillis() < deadline) delay(2)
    delay(20)
    vmScope.cancel(); uiScope.cancel()
    Outcome(crashed.get(), catchRan.get(), received.get())
}

fun main(args: Array<String>) {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    for ((replay, subscriber) in listOf(0 to true, 1 to true, 0 to false)) for (n in listOf(1, 10, 63, 64, 65, 66, 100)) {
        var crash = 0; var caught = 0; var minGot = Int.MAX_VALUE; var maxGot = 0
        repeat(runs) {
            val o = run(n, replay, subscriber)
            if (o.crashed) crash++; if (o.catchRan) caught++
            minGot = minOf(minGot, o.received); maxGot = maxOf(maxGot, o.received)
        }
        println("shareIn(replay = $replay), ${if (subscriber) "busy subscriber" else "no subscriber"}, $n values before the error: crashed $crash/$runs, catch ran $caught/$runs, sharing coroutine received $minGot to $maxGot of $n")
    }
}
