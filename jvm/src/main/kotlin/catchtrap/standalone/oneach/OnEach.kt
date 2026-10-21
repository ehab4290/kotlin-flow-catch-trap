package catchtrap.standalone.oneach

import catchtrap.standalone.notifystream.ChatNotifications
import catchtrap.standalone.notifystream.FakeRoom
import catchtrap.standalone.notifystream.Message
import catchtrap.standalone.notifystream.Row
import catchtrap.standalone.notifystream.SHOW_MS
import catchtrap.standalone.notifystream.toDomain
import catchtrap.standalone.notifystream.uncaught
import catchtrap.standalone.notifystream.waitUntil
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/*
 * Why does moving the work into onEach, before catch, keep catch working, and when does the work itself
 * finish? The notifier from notifystream, with probes in the repository and in the work.
 *
 * Part 1. A text arrives and is posted. A poll arrives right after posting, while the work waits, and the
 * mapper throws on it. Rows:
 *   collect { show; mark } after catch                                   the article's example, to compare
 *   onEach { show; mark }, catch, collect()                              the work before catch
 *   onEach, flowOn(Dispatchers.IO), catch: repository on Dispatchers.IO  the same dispatcher as the repository
 *   the same, repository on Dispatchers.Default                          a different dispatcher (it shares threads with IO)
 *   the same, repository on its own thread                               a different dispatcher and thread
 * Counted:
 *   crashed: the error reached the scope (the app would crash).
 *   catch ran.
 *   catch got the poll's error with nothing suppressed. If catch had written the work's cancellation into
 *     its notebook, rule 3 would rethrow instead, with the cancellation attached as suppressed.
 *   work cancelled: show() or markNotified threw a CancellationException.
 *   first text marked as notified: the work finished.
 *   repository ran ahead: the repository's emit() of the text returned before the text was marked, so a
 *     channel separated the source from the work. Without a channel, emit() returns only after the work.
 *
 * Part 2. A value still in the queue. The source emits "cached" and fails right after, collected on one
 * thread. Did "cached" reach the work?
 *   flowOn(IO), catch, collect { work }             to compare
 *   flowOn(IO), onEach { work }, catch, collect()   the work before catch
 *   catch, flowOn(IO), collect { work }             the fix
 */

/** The article's repository, with a probe after emit(). The probe is a transform, so it adds no channel. */
class ProbeRepository(private val room: FakeRoom, private val dispatcher: CoroutineDispatcher) {
    val ranAhead = AtomicBoolean(false)

    fun messagesToNotify(): Flow<List<Message>> =
        room.observeNotNotified()
            .map { rows -> rows.map { it.toDomain() } }
            .filter { it.isNotEmpty() }
            .transform { messages ->
                emit(messages)
                if (messages.any { it.id == "m1" } && !room.isNotified("m1")) ranAhead.set(true)
            }
            .flowOn(dispatcher)

    fun markNotified(ids: List<String>) = room.markNotified(ids)
}

enum class Shape(val label: String) {
    COLLECT("collect { show; mark } after catch"),
    ON_EACH("onEach { show; mark }, catch, collect()"),
    ON_EACH_FLOW_ON("onEach { show; mark }, flowOn(IO), catch, collect()"),
}

class ProbeNotifier(private val repository: ProbeRepository, private val notifications: ChatNotifications) {
    val caught = AtomicReference<Throwable?>(null)
    val workCancelled = AtomicBoolean(false)

    private suspend fun work(messages: List<Message>) {
        try {
            notifications.show(messages)
            repository.markNotified(messages.map { it.id })
        } catch (e: CancellationException) {
            workCancelled.set(true)
            throw e
        }
    }

    suspend fun observe(shape: Shape) {
        val toNotify = repository.messagesToNotify()
        when (shape) {
            Shape.COLLECT -> toNotify
                .catch { caught.set(it) } // log(it)
                .collect { work(it) }
            Shape.ON_EACH -> toNotify
                .onEach { work(it) }
                .catch { caught.set(it) } // log(it)
                .collect()
            Shape.ON_EACH_FLOW_ON -> toNotify
                .onEach { work(it) }
                .flowOn(Dispatchers.IO)
                .catch { caught.set(it) } // log(it)
                .collect()
        }
    }
}

class Run(
    val crashed: Boolean, val catchRan: Boolean, val cleanCatch: Boolean,
    val workCancelled: Boolean, val firstMarked: Boolean, val ranAhead: Boolean,
)

suspend fun oneRun(shape: Shape, dispatcher: CoroutineDispatcher): Run {
    uncaught.clear()
    val room = FakeRoom()
    val repository = ProbeRepository(room, dispatcher)
    val notifications = ChatNotifications()
    val notifier = ProbeNotifier(repository, notifications)
    notifications.afterFirstPosting = { room.insert(Row("m2", "POLL")) }   // while the work waits after posting
    // Once for the whole app, as Wire does: no exception handler.
    val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    appScope.launch { notifier.observe(shape) }

    room.insert(Row("m1", "TEXT"))
    waitUntil(20 * SHOW_MS) { notifications.posting.get() > 0 }
    delay(5 * SHOW_MS)
    val caught = notifier.caught.get()
    val run = Run(
        crashed = uncaught.isNotEmpty(),
        catchRan = caught != null,
        cleanCatch = caught is IllegalStateException && caught.suppressed.isEmpty(),
        workCancelled = notifier.workCancelled.get(),
        firstMarked = room.isNotified("m1"),
        ranAhead = repository.ranAhead.get(),
    )
    appScope.cancel()
    return run
}

fun part1(runs: Int) = runBlocking {
    println("Part 1: a poll arrives right after the text is posted, while the work waits")
    val ownThread = Executors.newSingleThreadExecutor { r -> Thread(r, "repository").apply { isDaemon = true } }
        .asCoroutineDispatcher()
    val rows = listOf(
        Triple(Shape.COLLECT, "repository on Dispatchers.IO", Dispatchers.IO),
        Triple(Shape.ON_EACH, "repository on Dispatchers.IO", Dispatchers.IO),
        Triple(Shape.ON_EACH_FLOW_ON, "repository on Dispatchers.IO", Dispatchers.IO),
        Triple(Shape.ON_EACH_FLOW_ON, "repository on Dispatchers.Default", Dispatchers.Default),
        Triple(Shape.ON_EACH_FLOW_ON, "repository on its own thread", ownThread),
    )
    for ((shape, repoLabel, dispatcher) in rows) {
        var crashed = 0; var catchRan = 0; var clean = 0; var cancelled = 0; var marked = 0; var ahead = 0
        repeat(runs) {
            val r = oneRun(shape, dispatcher)
            if (r.crashed) crashed++
            if (r.catchRan) catchRan++
            if (r.cleanCatch) clean++
            if (r.workCancelled) cancelled++
            if (r.firstMarked) marked++
            if (r.ranAhead) ahead++
        }
        println(
            "   %-52s %-34s | crashed %3d/%d | catch ran %3d | catch got the poll's error, nothing suppressed %3d | work cancelled %3d | first text marked %3d | repository ran ahead %3d"
                .format(shape.label, repoLabel, crashed, runs, catchRan, clean, cancelled, marked, ahead)
        )
    }
    ownThread.close()
}

fun part2(runs: Int) = runBlocking {
    println("Part 2: the source emits \"cached\" and fails right after, collected on one thread")
    val rows = listOf("flowOn(IO), catch, collect { work }", "flowOn(IO), onEach { work }, catch, collect()", "catch, flowOn(IO), collect { work }")
    for ((i, label) in rows.withIndex()) {
        var kept = 0; var catchRan = 0; var escaped = 0
        repeat(runs) {
            var got = false
            var ran = false
            val source = flow {
                emit("cached")
                throw IOException("offline")
            }
            try {
                when (i) {
                    0 -> source.flowOn(Dispatchers.IO).catch { ran = true }.collect { got = true }
                    1 -> source.flowOn(Dispatchers.IO).onEach { got = true }.catch { ran = true }.collect()
                    else -> source.catch { ran = true }.flowOn(Dispatchers.IO).collect { got = true }
                }
            } catch (e: Throwable) {
                escaped++
            }
            if (got) kept++
            if (ran) catchRan++
        }
        println("   %-48s | \"cached\" reached the work %3d/%d | catch ran %3d | error escaped %3d".format(label, kept, runs, catchRan, escaped))
    }
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
    println("kotlinx.coroutines 1.11.0, $runs runs per row, posting waits $SHOW_MS ms")
    part1(runs)
    part2(runs)
}
