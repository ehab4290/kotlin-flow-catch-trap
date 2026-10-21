package catchtrap.standalone.handoff

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

// The ViewModel collects the repository's flow (catch after flowOn) and hands each update to the UI.
// The source emits n updates quickly, then fails. The UI is either busy on every update (a snackbar)
// or not collecting at all (the app is in the background).
// In the "at once" rows the source fails only once the ViewModel has received all n updates (or after
// 500 ms), so a value still in flowOn's own queue can't decide the row: what's measured is the handoff.
enum class Handoff { CHANNEL, CHANNEL_BUFFERED, MUTABLE_SHARED_FLOW, SHARE_IN, MUTABLE_STATE_FLOW, STATE_IN }

class Outcome(val crashed: Boolean, val catchRan: Boolean, val received: Int)

fun run(h: Handoff, n: Int, uiBusy: Boolean, spaced: Boolean): Outcome = runBlocking {
    val crashed = AtomicBoolean(false); val catchRan = AtomicBoolean(false); val received = AtomicInteger()
    val vm = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, _ -> crashed.set(true) })
    val ui = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    val safe = flow {
        repeat(n) { emit(it); if (spaced) delay(50) }
        if (!spaced) withTimeoutOrNull(500) { while (received.get() < n) delay(1) }   // until the ViewModel has them all
        throw IOException("network down")
    }
        .flowOn(Dispatchers.IO).catch { catchRan.set(true); emit(-1) }
        .onEach { if (it >= 0) received.incrementAndGet() }                         // what the ViewModel received
    val busy: suspend (Any) -> Unit = { delay(10_000) }
    when (h) {
        Handoff.CHANNEL, Handoff.CHANNEL_BUFFERED -> {
            val ch = Channel<Int>(if (h == Handoff.CHANNEL) Channel.RENDEZVOUS else Channel.BUFFERED)
            if (uiBusy) ui.launch { ch.receiveAsFlow().collect(busy) }
            delay(5); vm.launch { safe.collect { ch.send(it) } }
        }
        Handoff.MUTABLE_SHARED_FLOW -> {
            val sf = MutableSharedFlow<Int>()
            if (uiBusy) ui.launch { sf.collect(busy) }
            delay(5); vm.launch { safe.collect { sf.emit(it) } }
        }
        Handoff.SHARE_IN -> {
            val s = safe.shareIn(vm, if (uiBusy) SharingStarted.Lazily else SharingStarted.Eagerly)
            if (uiBusy) ui.launch { s.collect(busy) }
        }
        Handoff.MUTABLE_STATE_FLOW -> {
            val st = MutableStateFlow(0)
            if (uiBusy) ui.launch { st.collect(busy) }
            delay(5); vm.launch { safe.collect { st.value = it } }
        }
        Handoff.STATE_IN -> {
            val s = safe.stateIn(vm, if (uiBusy) SharingStarted.Lazily else SharingStarted.Eagerly, 0)
            if (uiBusy) ui.launch { s.collect(busy) }
        }
    }
    val deadline = System.currentTimeMillis() + 2_500
    while (!crashed.get() && !catchRan.get() && System.currentTimeMillis() < deadline) delay(2)
    delay(20)
    vm.cancel(); ui.cancel()
    Outcome(crashed.get(), catchRan.get(), received.get())
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    // A few updates, 50 ms apart, as on the order screen. Then 100 updates at once: a pile-up.
    for ((n, spaced) in listOf(3 to true, 100 to false)) for (h in Handoff.entries) for (uiBusy in listOf(true, false)) {
        if (!spaced && (h == Handoff.CHANNEL || h == Handoff.MUTABLE_SHARED_FLOW)) continue // no buffer: the source can't get ahead
        var crash = 0; var caught = 0; var minGot = Int.MAX_VALUE; var maxGot = 0
        repeat(runs) {
            val o = run(h, n, uiBusy, spaced)
            if (o.crashed) crash++; if (o.catchRan) caught++
            minGot = minOf(minGot, o.received); maxGot = maxOf(maxGot, o.received)
        }
        val got = if (spaced) "" else ", ViewModel received $minGot to $maxGot of $n"
        println("${if (spaced) "3 updates, 50 ms apart" else "100 updates at once"} | $h, ${if (uiBusy) "UI busy" else "background"}: crashed $crash/$runs, catch ran $caught/$runs$got")
    }
}
