package catchtrap.standalone.whilesub

import catchtrap.standalone.ways.UpstreamError
import catchtrap.standalone.ways.newFakeMainDispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/*
 * Part 1. Where does an upstream error go with stateIn / shareIn? The scope has a name and a handler
 * (viewModelScope plus a crash counter); a "screen" coroutine subscribes with a catch of its own. The
 * source emits "cached", then fails 5 ms later. Counted: whether the error reached the scope's handler,
 * whether the coroutine that failed belonged to the scope, whether the subscriber is still waiting
 * 100 ms later, whether the subscriber's catch ran, and whether it got "cached".
 *
 * Part 2. stateIn / shareIn with SharingStarted.WhileSubscribed: what happens when the last subscriber leaves
 * while the source is in a blocking call that then fails?
 *
 * The sharing coroutine runs in a scope with a handler (viewModelScope plus a crash counter), on one
 * main thread. A screen subscribes, then leaves 2 ms later, while the source is in a 5 ms blocking call
 * on Dispatchers.IO. WhileSubscribed(0) then stops the upstream collection, and the call fails.
 * The "stays" rows are the control: the screen is still subscribed when the source fails.
 *
 * Part 3. With catch { emit("fallback") } after flowOn, before stateIn / shareIn(replay = 1): what do the
 * subscribers see? Same source (emit, 5 ms blocking call, fail). Counted: crashes, whether the subscriber
 * got the fallback, whether it is still waiting 50 ms later, and the value the shared flow holds afterwards
 * (state.value, or the replay cache for shareIn), which a screen that subscribes later sees first.
 */

private enum class Placement(val label: String) {
    NONE("no catch"),
    AFTER_FLOWON("catch after flowOn, before stateIn/shareIn"),
    BEFORE_FLOWON("catch before flowOn"),
}

private enum class Sharing { STATE_IN, SHARE_IN }

class Counts {
    var crashed = 0; var catchRan = 0
    override fun toString() = "crashed %3d | catch ran %3d".format(crashed, catchRan)
}

private fun upstream(p: Placement, c: AtomicInteger): Flow<String> {
    val source = flow { emit("cached"); Thread.sleep(5); throw UpstreamError() }
    val handler: suspend kotlinx.coroutines.flow.FlowCollector<String>.(Throwable) -> Unit = { c.incrementAndGet(); emit("fallback") }
    return when (p) {
        Placement.NONE -> source.flowOn(Dispatchers.IO)
        Placement.AFTER_FLOWON -> source.flowOn(Dispatchers.IO).catch(handler)
        Placement.BEFORE_FLOWON -> source.catch(handler).flowOn(Dispatchers.IO)
    }
}

private suspend fun run(p: Placement, s: Sharing, leaves: Boolean, runs: Int, main: CoroutineDispatcher): Counts {
    val out = Counts()
    repeat(runs) {
        val errors = CopyOnWriteArrayList<Throwable>()
        val c = AtomicInteger()
        val viewModelScope = CoroutineScope(SupervisorJob() + main + CoroutineExceptionHandler { _, e -> errors += e })
        val shared: Flow<String> = when (s) {
            Sharing.STATE_IN -> upstream(p, c).stateIn(viewModelScope, SharingStarted.WhileSubscribed(0), "loading")
            Sharing.SHARE_IN -> upstream(p, c).shareIn(viewModelScope, SharingStarted.WhileSubscribed(0), replay = 1)
        }
        val screen = CoroutineScope(SupervisorJob() + main)
        screen.launch { shared.collect { } }
        delay(2)
        if (leaves) screen.cancel()
        delay(30)
        if (errors.isNotEmpty()) out.crashed++
        if (c.get() > 0) out.catchRan++
        screen.cancel(); viewModelScope.cancel()
    }
    return out
}


class WhereCounts {
    var scopeGotError = 0; var failedInScope = 0; var subscriberWaiting = 0; var subscriberCatchRan = 0; var subscriberGotCached = 0
    override fun toString() = "error reached the scope's handler %3d | failed coroutine belonged to the scope %3d | subscriber still waiting after 100 ms %3d | subscriber's catch ran %3d | subscriber got cached %3d"
        .format(scopeGotError, failedInScope, subscriberWaiting, subscriberCatchRan, subscriberGotCached)
}

private suspend fun whereDoesTheErrorGo(kind: String, runs: Int, main: CoroutineDispatcher): WhereCounts {
    val out = WhereCounts()
    repeat(runs) {
        val errors = CopyOnWriteArrayList<String>()
        val viewModelScope = CoroutineScope(SupervisorJob() + main + CoroutineName("viewModelScope") +
            CoroutineExceptionHandler { ctx, _ -> errors += (ctx[CoroutineName]?.name ?: "?") })
        val source = flow { emit("cached"); delay(5); throw UpstreamError() }.flowOn(Dispatchers.IO)
        val hot: Flow<String> = when (kind) {
            "stateIn(Eagerly)" -> source.stateIn(viewModelScope, SharingStarted.Eagerly, "loading")
            "stateIn(WhileSubscribed(5_000))" -> source.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), "loading")
            else -> source.shareIn(viewModelScope, SharingStarted.Eagerly, replay = 1)
        }
        var catchRan = false; var gotCached = false
        val screen = CoroutineScope(SupervisorJob() + main)
        val subscriber = screen.launch(CoroutineName("subscriber")) {
            hot.catch { catchRan = true }.collect { if (it == "cached") gotCached = true }
        }
        delay(100)
        if (errors.isNotEmpty()) out.scopeGotError++
        if (errors.any { it == "viewModelScope" }) out.failedInScope++
        if (subscriber.isActive) out.subscriberWaiting++
        if (catchRan) out.subscriberCatchRan++
        if (gotCached) out.subscriberGotCached++
        screen.cancel(); viewModelScope.cancel()
    }
    return out
}

class NotifyCounts {
    var crashed = 0; var gotFallback = 0; var stillWaiting = 0; val heldAfter = sortedMapOf<String, Int>()
    override fun toString() = "crashed %3d | subscriber got fallback %3d | subscriber still waiting %3d | held afterwards %s"
        .format(crashed, gotFallback, stillWaiting, heldAfter)
}

private suspend fun whatSubscribersSee(s: Sharing, started: String, runs: Int, main: CoroutineDispatcher): NotifyCounts {
    val out = NotifyCounts()
    repeat(runs) {
        val errors = CopyOnWriteArrayList<Throwable>()
        val viewModelScope = CoroutineScope(SupervisorJob() + main + CoroutineExceptionHandler { _, e -> errors += e })
        val upstream = flow { emit("cached"); Thread.sleep(5); throw UpstreamError() }
            .flowOn(Dispatchers.IO).catch { emit("fallback") }
        val strategy = if (started == "Eagerly") SharingStarted.Eagerly else SharingStarted.WhileSubscribed(0)
        val held: () -> String
        val shared: Flow<String> = when (s) {
            Sharing.STATE_IN -> upstream.stateIn(viewModelScope, strategy, "loading").also { st -> held = { st.value } }
            Sharing.SHARE_IN -> upstream.shareIn(viewModelScope, strategy, replay = 1).also { sh -> held = { sh.replayCache.lastOrNull() ?: "nothing" } }
        }
        val seen = CopyOnWriteArrayList<String>()
        val screen = CoroutineScope(SupervisorJob() + main)
        val subscriber = screen.launch { shared.collect { seen += it } }
        delay(2)
        val leaves = started.contains("leaves")
        if (leaves) screen.cancel()
        delay(50)
        if (errors.isNotEmpty()) out.crashed++
        if ("fallback" in seen) out.gotFallback++
        if (subscriber.isActive) out.stillWaiting++
        out.heldAfter.merge(held(), 1, Int::plus)
        screen.cancel(); viewModelScope.cancel()
    }
    return out
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    println("kotlinx.coroutines 1.11.0, $runs runs per row, collected on one main thread")
    newFakeMainDispatcher().use { main ->
        runBlocking(main) {
            println("== part 1: where the upstream error goes (no catch before stateIn/shareIn)")
            for (kind in listOf("stateIn(Eagerly)", "stateIn(WhileSubscribed(5_000))", "shareIn(Eagerly)"))
                println("   %-32s %s".format(kind, whereDoesTheErrorGo(kind, runs, main)))
            println("== part 2: WhileSubscribed(0), the screen leaves during a blocking call")
            for (s in Sharing.entries) for (leaves in listOf(true, false)) {
                println("== ${if (s == Sharing.STATE_IN) "stateIn" else "shareIn(replay = 1)"}, ${if (leaves) "the screen leaves during the blocking call" else "the screen stays (control)"}")
                for (p in Placement.entries) println("   %-46s %s".format(p.label, run(p, s, leaves, runs, main)))
            }
            println("== part 3: catch { emit(\"fallback\") } after flowOn, before stateIn/shareIn: what the subscribers see")
            for (s in Sharing.entries) for (started in listOf("Eagerly", "WhileSubscribed(0), screen stays", "WhileSubscribed(0), screen leaves during the call"))
                println("   %-20s %-48s %s".format(if (s == Sharing.STATE_IN) "stateIn" else "shareIn(replay = 1)", started, whatSubscribersSee(s, started, runs, main)))
        }
    }
}
