package catchtrap.standalone.notifystream

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/*
 * The article's lead example: the chat app's notifications, shown the way Wire shows them
 * (WireNotificationManager.observeLocalNotifications in wire-android). MessageNotifier collects the
 * messages that have no notification yet, posts a notification for them, then marks them as notified in
 * the database, and the query emits again without them. Wire's handleNotification waits 100 ms after
 * posting, "to let notification manager update activeNotifications list" (MessageNotificationManager.kt).
 * Here that wait is SHOW_MS, scaled down like the other JVM checks.
 *
 * The observer runs in an app-wide scope with no exception handler, as Wire's does
 * (CoroutineScope(SupervisorJob() + Dispatchers.Default)): an error that reaches it crashes the app.
 * Here a default uncaught-exception handler counts it instead.
 *
 * The source stands in for Room: it re-runs the query and emits again after every write to the table, and
 * it never completes. The repository maps rows to the domain model and moves the work off the main thread
 * with flowOn.
 *
 * Part 1, the crash. Each run: a text message arrives and its notification is posted. Then a poll arrives
 * (the mapper throws on it), either right after posting, so the observer is still waiting, or after the
 * text was marked as notified.
 *   catch in the observer, after flowOn: collect { show; markNotified }   the article's example
 *   catch in the repository, before flowOn                                 the fix
 *   onEach { show; markNotified }, catch, collect()                        the work before catch
 *   the same with flowOn(IO) after onEach
 * Counted: the error reached the scope (the app would crash), each catch ran, and crashes whose trace has
 * the article's fingerprint: a suppressed cancellation caused by the crash itself, which the trace prints
 * as "Caused by: [CIRCULAR REFERENCE: ...]".
 *
 * Part 2, after the fix. A text, then a poll, then another text. Is the second text notified?
 *   catch before flowOn          the poll ends the flow
 *   per message, then catch      the poll is skipped inside map; catch before flowOn stays for the rest
 * Counted: first text notified, observer ended, text after the poll notified, how often the poll was
 * logged, and new logs once nothing writes to the table any more (a loop would add them).
 *
 * Part 3, collectLatest instead of collect, catch still after flowOn. A poll or a second text arrives
 * while the observer waits after posting the first. Counted: crashes, catch ran, and runs in which the
 * first text was posted twice. Plain collect with a second text is the comparison: the newer list was
 * read before the first text was marked as notified, so it holds the first text either way.
 */

const val SHOW_MS = 10L

data class Row(val id: String, val type: String, val notified: Boolean = false)
data class Message(val id: String)

/** This app version knows text messages and throws on any other type, such as a poll added later. */
fun Row.toDomain(): Message = if (type == "TEXT") Message(id) else error("Unknown message type: $type")

/** Stands in for Room: every write invalidates the query, which runs again and emits the rows not notified yet. */
class FakeRoom {
    private val rows = mutableListOf<Row>()
    private val invalidations = MutableSharedFlow<Unit>(replay = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST)
        .also { it.tryEmit(Unit) }
    val writes = AtomicInteger()

    fun observeNotNotified(): Flow<List<Row>> = invalidations.map { synchronized(rows) { rows.filter { !it.notified } } }
    fun insert(row: Row) = write { rows += row }
    fun markNotified(ids: List<String>) = write { rows.replaceAll { if (it.id in ids) it.copy(notified = true) else it } }
    fun isNotified(id: String) = synchronized(rows) { rows.first { it.id == id }.notified }

    private fun write(change: () -> Unit) {
        synchronized(rows) { change() }
        writes.incrementAndGet()
        invalidations.tryEmit(Unit)
    }
}

/** Posts the notifications, then waits SHOW_MS, as Wire's handleNotification does. */
class ChatNotifications {
    val posting = AtomicInteger()
    /** How many times each message was posted. */
    val posted = ConcurrentHashMap<String, Int>()

    /** For the checks: runs once, right after the first posting, so a message can arrive during the wait. */
    @Volatile var afterFirstPosting: (() -> Unit)? = null

    suspend fun show(messages: List<Message>) {
        posting.incrementAndGet()
        messages.forEach { posted.merge(it.id, 1, Int::plus) }   // notificationManager.notify(...)
        afterFirstPosting?.let { afterFirstPosting = null; it() }
        delay(SHOW_MS)                                           // delay(NOTIFICATION_UPDATE_DELAY)
    }
}

class MessageRepository(private val room: FakeRoom, private val io: CoroutineDispatcher = Dispatchers.IO) {
    /** How often the mapper's error was logged, by the repository's catch or by the per-message try/catch. */
    val logged = AtomicInteger()

    /** The article's repository: map to the domain model, then move the work off the main thread. */
    fun messagesToNotify(): Flow<List<Message>> =
        room.observeNotNotified()
            .map { rows -> rows.map { it.toDomain() } }
            .filter { it.isNotEmpty() }
            .flowOn(io)

    /** The fix: catch before flowOn. */
    fun messagesToNotifyCatchBeforeFlowOn(): Flow<List<Message>> =
        room.observeNotNotified()
            .map { rows -> rows.map { it.toDomain() } }
            .filter { it.isNotEmpty() }
            .catch { logged.incrementAndGet() } // log(it)
            .flowOn(io)

    /** An expected error handled per message, and catch before flowOn for everything else. */
    fun messagesToNotifyPerMessage(): Flow<List<Message>> =
        room.observeNotNotified()
            .map { rows ->
                rows.mapNotNull { row ->
                    try { row.toDomain() } catch (e: IllegalStateException) { logged.incrementAndGet(); null } // log(e)
                }
            }
            .filter { it.isNotEmpty() }
            .catch { logged.incrementAndGet() } // log(it)
            .flowOn(io)

    fun markNotified(ids: List<String>) = room.markNotified(ids)   // Room emits again, without these messages
}

enum class Shape(val label: String) {
    COLLECT("catch in the observer, after flowOn"),
    CATCH_IN_REPOSITORY("catch in the repository, before flowOn"),
    ON_EACH("onEach { show; mark }, catch, collect()"),
    ON_EACH_FLOW_ON("onEach { show; mark }, flowOn(IO), catch"),
    COLLECT_LATEST("catch, then collectLatest { show; mark }"),
}

/** The article's MessageNotifier, with the shapes the checks compare. */
class MessageNotifier(
    private val repository: MessageRepository,
    private val notifications: ChatNotifications,
    private val toNotify: Flow<List<Message>> = repository.messagesToNotify(),
) {
    val catchRan = AtomicInteger()

    suspend fun observe(shape: Shape) {
        when (shape) {
            Shape.COLLECT, Shape.CATCH_IN_REPOSITORY -> toNotify
                .catch { catchRan.incrementAndGet() } // log(it)
                .collect { messages ->
                    notifications.show(messages)
                    repository.markNotified(messages.map { it.id })
                }
            Shape.ON_EACH -> toNotify
                .onEach { messages ->
                    notifications.show(messages)
                    repository.markNotified(messages.map { it.id })
                }
                .catch { catchRan.incrementAndGet() } // log(it)
                .collect()
            Shape.ON_EACH_FLOW_ON -> toNotify
                .onEach { messages ->
                    notifications.show(messages)
                    repository.markNotified(messages.map { it.id })
                }
                .flowOn(Dispatchers.IO)
                .catch { catchRan.incrementAndGet() } // log(it)
                .collect()
            Shape.COLLECT_LATEST -> toNotify
                .catch { catchRan.incrementAndGet() } // log(it)
                .collectLatest { messages ->
                    notifications.show(messages)
                    repository.markNotified(messages.map { it.id })
                }
        }
    }
}

suspend fun waitUntil(timeoutMs: Long, done: () -> Boolean) {
    var waited = 0L
    while (!done() && waited < timeoutMs) { delay(2); waited += 2 }
}

val uncaught = CopyOnWriteArrayList<Throwable>()

class Run(val crashed: Boolean, val crash: Throwable?, val catchRan: Boolean, val repositoryLogged: Boolean, val firstPostedTwice: Boolean) {
    /** The trace's fingerprint: a suppressed cancellation caused by the crash itself (CIRCULAR REFERENCE). */
    val fingerprint: Boolean
        get() = crash != null && crash.suppressed.any { it is CancellationException && it.cause === crash } &&
            "CIRCULAR REFERENCE" in crash.stackTraceToString()
}

/** One run: m1 TEXT arrives, then m2 (`arriving`) right after m1 is posted, while the observer waits, or after m1 was marked. */
suspend fun oneRun(shape: Shape, arriving: String, whileWaiting: Boolean): Run {
    uncaught.clear()
    val room = FakeRoom()
    val repository = MessageRepository(room)
    val notifications = ChatNotifications()
    val toNotify = if (shape == Shape.CATCH_IN_REPOSITORY) repository.messagesToNotifyCatchBeforeFlowOn() else repository.messagesToNotify()
    val notifier = MessageNotifier(repository, notifications, toNotify)
    if (whileWaiting) notifications.afterFirstPosting = { room.insert(Row("m2", arriving)) }
    // Once for the whole app, as Wire does: no exception handler.
    val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    appScope.launch { notifier.observe(shape) }

    room.insert(Row("m1", "TEXT"))
    waitUntil(20 * SHOW_MS) { notifications.posting.get() > 0 }
    if (!whileWaiting) {
        waitUntil(20 * SHOW_MS) { room.isNotified("m1") }
        delay(2 * SHOW_MS)
        room.insert(Row("m2", arriving))
    }
    delay(5 * SHOW_MS)
    val run = Run(uncaught.isNotEmpty(), uncaught.firstOrNull(), notifier.catchRan.get() > 0, repository.logged.get() > 0,
        (notifications.posted["m1"] ?: 0) > 1)
    appScope.cancel()
    return run
}

fun part1(runs: Int) = runBlocking {
    println("Part 1: a poll arrives. The observer collects in CoroutineScope(SupervisorJob() + Dispatchers.Default)")
    for (shape in listOf(Shape.COLLECT, Shape.CATCH_IN_REPOSITORY, Shape.ON_EACH, Shape.ON_EACH_FLOW_ON)) {
        for ((timing, whileWaiting) in listOf("right after posting, while the observer waits" to true, "after the text was marked as notified" to false)) {
            var crashed = 0; var observerCatch = 0; var repositoryCatch = 0; var fingerprint = 0
            var sampleSuppressed: Throwable? = null
            val types = sortedMapOf<String, Int>()
            repeat(runs) {
                val r = oneRun(shape, "POLL", whileWaiting)
                if (r.crashed) {
                    crashed++
                    types.merge(r.crash!!.javaClass.simpleName + ": " + r.crash.message, 1, Int::plus)
                    if (r.fingerprint) fingerprint++
                    if (sampleSuppressed == null) sampleSuppressed = r.crash.suppressed.firstOrNull()
                }
                if (r.catchRan) observerCatch++
                if (r.repositoryLogged) repositoryCatch++
            }
            val crashes = if (types.isEmpty()) "" else
                " | $types | trace with Suppressed + CIRCULAR REFERENCE $fingerprint/$crashed | Suppressed: ${sampleSuppressed?.toString()?.substringBefore(";")}"
            println("   %-42s poll arrives %-45s crashed %3d/%d | observer's catch ran %3d | repository's catch ran %3d%s"
                .format(shape.label, timing, crashed, runs, observerCatch, repositoryCatch, crashes))
        }
    }
}

class Outcome(val runs: Int) {
    var firstNotified = 0; var observerEnded = 0; var afterPollNotified = 0; var logsWhenQuiet = 0; var crashed = 0
    var minLogs = Int.MAX_VALUE; var maxLogs = 0
    override fun toString() = "crashed %d/%d | first text notified %3d/%d | observer ended %3d/%d | text after the poll notified %3d/%d | poll logged %d to %d times per run | new logs once the table was quiet %d"
        .format(crashed, runs, firstNotified, runs, observerEnded, runs, afterPollNotified, runs, minLogs, maxLogs, logsWhenQuiet)
}

fun part2(runs: Int) = runBlocking {
    println("Part 2: after the fix. A text, a poll, then another text")
    for ((label, perMessage) in listOf("catch before flowOn" to false, "per message, then catch" to true)) {
        val out = Outcome(runs)
        repeat(runs) {
            uncaught.clear()
            val room = FakeRoom()
            val repository = MessageRepository(room)
            val notifications = ChatNotifications()
            val source = if (perMessage) repository.messagesToNotifyPerMessage() else repository.messagesToNotifyCatchBeforeFlowOn()
            val notifier = MessageNotifier(repository, notifications, source)
            val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val observer = appScope.launch { notifier.observe(Shape.COLLECT) }

            room.insert(Row("m1", "TEXT"))
            waitUntil(20 * SHOW_MS) { room.isNotified("m1") }
            if (room.isNotified("m1")) out.firstNotified++
            room.insert(Row("m2", "POLL"))
            delay(5 * SHOW_MS)
            room.insert(Row("m3", "TEXT"))
            waitUntil(20 * SHOW_MS) { room.isNotified("m3") }
            delay(5 * SHOW_MS)
            if (room.isNotified("m3") && notifications.posted.containsKey("m3")) out.afterPollNotified++
            if (observer.isCompleted) out.observerEnded++

            val logs = repository.logged.get()
            val writes = room.writes.get()
            delay(20 * SHOW_MS)                                   // nothing writes to the table now
            check(room.writes.get() == writes) { "the table changed while it should be quiet" }
            out.logsWhenQuiet += repository.logged.get() - logs
            out.minLogs = minOf(out.minLogs, logs); out.maxLogs = maxOf(out.maxLogs, logs)
            if (uncaught.isNotEmpty()) out.crashed++
            appScope.cancel()
        }
        println("   %-24s %s".format(label, out))
    }
}

fun part3(runs: Int) = runBlocking {
    println("Part 3: collectLatest, catch after flowOn, and plain collect to compare. A second message arrives right after the first is posted, while the observer waits")
    for ((shape, arriving) in listOf(Shape.COLLECT_LATEST to "POLL", Shape.COLLECT_LATEST to "TEXT", Shape.COLLECT to "TEXT")) {
        var crashed = 0; var catchRan = 0; var postedTwice = 0
        repeat(runs) {
            val r = oneRun(shape, arriving, whileWaiting = true)
            if (r.crashed) crashed++
            if (r.catchRan) catchRan++
            if (r.firstPostedTwice) postedTwice++
        }
        val label = if (shape == Shape.COLLECT) "plain collect { show; mark } (comparison)" else shape.label
        println("   %-42s %-5s arrives: crashed %3d/%d | catch ran %3d | first text posted twice %3d/%d"
            .format(label, arriving.lowercase(), crashed, runs, catchRan, postedTwice, runs))
    }
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
    println("kotlinx.coroutines 1.11.0, $runs runs per row, posting waits $SHOW_MS ms")
    part1(runs)
    part2(runs)
    part3(runs)
}
