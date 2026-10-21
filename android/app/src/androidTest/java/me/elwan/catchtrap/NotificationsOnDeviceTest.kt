package me.elwan.catchtrap

import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import me.elwan.catchtrap.notify.ChatNotifications
import me.elwan.catchtrap.notify.MessageEntity
import me.elwan.catchtrap.notify.MessageNotifier
import me.elwan.catchtrap.notify.MessageRepository
import me.elwan.catchtrap.notify.NotificationDatabase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The article's lead example with real parts: a Room database, a Room Flow of the messages with no
 * notification yet, and [MessageNotifier] running as Wire's observer does (WireNotificationManager in
 * wire-android): once, in an app-wide CoroutineScope(SupervisorJob() + Dispatchers.Default) with no
 * exception handler. Posting a notification is stood in for by [ChatNotifications], which then waits
 * [WAIT_MS], as Wire's MessageNotificationManager.handleNotification waits 100 ms after posting.
 *
 * Each run: a text message arrives and the observer posts its notification. Then a second message
 * arrives, either while the observer is still waiting after posting, or after the text was marked as
 * notified. Usually it is a poll (this app version's mapper throws on it); in one collectLatest row it is
 * a text. Then a third message, a text, arrives: is it notified?
 *
 * The only thing replaced is the handler that would kill the process, so the runs can be counted.
 *
 *     CLASS=me.elwan.catchtrap.NotificationsOnDeviceTest ./run-device-tests.sh 1 20
 */
@RunWith(AndroidJUnit4::class)
class NotificationsOnDeviceTest {

    private val context = InstrumentationRegistry.getInstrumentation().targetContext

    private enum class Source(val label: String) {
        CATCH_AFTER_FLOW_ON("catch in the observer, after flowOn"),
        CATCH_BEFORE_FLOW_ON("catch in the repository, before flowOn"),
        PER_MESSAGE("per message in the repository, then catch before flowOn"),
    }

    private enum class Collector(val label: String) {
        COLLECT("collect"),
        COLLECT_LATEST("collectLatest"),
        ON_EACH("onEach before catch, then collect()"),
        ON_EACH_FLOW_ON("onEach, then flowOn(IO), before catch, then collect()"),
    }

    private data class Row(
        val source: Source,
        val whileWaiting: Boolean,
        val collector: Collector = Collector.COLLECT,
        val arriving: String = "POLL",
    ) {
        val label: String
            get() = "[notifications: ${source.label}, ${collector.label}] " +
                (if (arriving == "POLL") "poll arrives " else "text arrives ") +
                if (whileWaiting) "while the observer waits after posting" else "after the text was marked as notified"
    }

    private data class Result(
        val row: Row,
        val runs: Int,
        val crashes: Int,
        val observerCatchRan: Int,
        /** Runs in which the repository logged the poll, by its catch or per message. */
        val repositoryLogged: Int,
        val minLogs: Int,
        val maxLogs: Int,
        val firstNotified: Int,
        val afterPollNotified: Int,
        val observerEnded: Int,
        /** Runs in which the first text was posted more than once. */
        val firstPostedTwice: Int,
        /**
         * Crashes whose trace has the article's fingerprint: a suppressed cancellation caused by the crash
         * itself, which the trace prints as `Caused by: [CIRCULAR REFERENCE: ...]`.
         */
        val fingerprint: Int,
        val sampleCrash: Throwable?,
    ) {
        fun summary() = "${row.label} | crashed $crashes/$runs, observer's catch ran $observerCatchRan, " +
            "repository logged the poll in $repositoryLogged runs ($minLogs to $maxLogs times per run), " +
            "first text notified $firstNotified/$runs, text after the poll notified $afterPollNotified/$runs, " +
            "observer ended $observerEnded/$runs, first text posted twice $firstPostedTwice/$runs" +
            (sampleCrash?.let { crash ->
                " | ${crash.javaClass.simpleName}: ${crash.message} | trace with Suppressed + CIRCULAR REFERENCE $fingerprint/$crashes" +
                    crash.suppressed.firstOrNull()?.let { " | Suppressed: $it" }.orEmpty()
            } ?: "")
    }

    private fun measure(row: Row, runs: Int = RUNS): Result = runBlocking {
        val uncaught = CopyOnWriteArrayList<Throwable>()
        val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
        var crashes = 0
        var observerCatchRan = 0
        var repositoryLogged = 0
        var minLogs = Int.MAX_VALUE
        var maxLogs = 0
        var firstNotified = 0
        var afterPollNotified = 0
        var observerEnded = 0
        var firstPostedTwice = 0
        var fingerprint = 0
        var sample: Throwable? = null
        try {
            repeat(runs) {
                uncaught.clear()
                val database = NotificationDatabase.inMemory(context)
                val dao = database.messageDao()
                val repository = MessageRepository(dao)
                val notifications = ChatNotifications(WAIT_MS)
                val toNotify = when (row.source) {
                    Source.CATCH_AFTER_FLOW_ON -> repository.messagesToNotify()
                    Source.CATCH_BEFORE_FLOW_ON -> repository.messagesToNotifyCaughtBeforeFlowOn()
                    Source.PER_MESSAGE -> repository.messagesToNotifyPerMessage()
                }
                val notifier = MessageNotifier(repository, notifications, toNotify)

                // Once for the whole app, as Wire does: CoroutineScope(SupervisorJob() + Dispatchers.Default), no handler.
                val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
                val job = appScope.launch {
                    when (row.collector) {
                        Collector.COLLECT -> notifier.observe()
                        Collector.COLLECT_LATEST -> notifier.observeWithCollectLatest()
                        Collector.ON_EACH -> notifier.observeInOnEach()
                        Collector.ON_EACH_FLOW_ON -> notifier.observeInOnEachWithFlowOn()
                    }
                }
                // Let Room run its first query on the empty table. Otherwise that query can race the insert below
                // and emit the first text twice, which would count as "posted twice" in every row.
                delay(300)

                dao.insert(MessageEntity(id = "m1", chatId = "c1", type = "TEXT", body = "Are you coming?"))
                waitFor(5_000) { notifications.posting.get() > 0 }
                if (row.whileWaiting) {
                    delay(WAIT_MS / 3)                                   // still waiting after posting
                } else {
                    waitFor(5_000) { dao.countNotified(listOf("m1")) == 1 }
                    delay(300)                                           // m1 is done, the observer is idle
                }
                val body = if (row.arriving == "POLL") "Pizza or sushi?" else "See you there"
                dao.insert(MessageEntity(id = "m2", chatId = "c1", type = row.arriving, body = body))
                delay(WAIT_MS + 500)
                dao.insert(MessageEntity(id = "m3", chatId = "c1", type = "TEXT", body = "On my way"))
                waitFor(5 * WAIT_MS + 1_000) { dao.countNotified(listOf("m3")) == 1 }
                delay(200)

                if (uncaught.isNotEmpty()) {
                    crashes++
                    val crash = uncaught.first()
                    if (sample == null) sample = crash
                    if (crash.suppressed.any { it is CancellationException && it.cause === crash } &&
                        "CIRCULAR REFERENCE" in crash.stackTraceToString()
                    ) fingerprint++
                }
                if (notifier.fallbackRan.get() > 0) observerCatchRan++
                val logs = repository.mapperFailures.get()
                if (logs > 0) repositoryLogged++
                minLogs = minOf(minLogs, logs)
                maxLogs = maxOf(maxLogs, logs)
                if (dao.countNotified(listOf("m1")) == 1) firstNotified++
                if (dao.countNotified(listOf("m3")) == 1 && notifications.posted.containsKey("m3")) afterPollNotified++
                if (job.isCompleted) observerEnded++
                if ((notifications.posted["m1"] ?: 0) > 1) firstPostedTwice++
                // Wait for the observer to stop before closing the database, so no query is still running.
                withTimeoutOrNull(2_000) { appScope.coroutineContext[Job]!!.cancelAndJoin() }
                database.close()
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(systemHandler)
        }
        Result(row, runs, crashes, observerCatchRan, repositoryLogged, minLogs, maxLogs, firstNotified,
            afterPollNotified, observerEnded, firstPostedTwice, fingerprint, sample)
            .also { Log.i(TAG, it.summary()) }
    }

    private suspend fun waitFor(timeoutMs: Long, done: suspend () -> Boolean) {
        var waited = 0L
        while (!done() && waited < timeoutMs) {
            delay(20)
            waited += 20
        }
    }

    /** catch after flowOn, both timings, and the per-message fix. Asserts nothing, so one surprising row doesn't stop the others. */
    @Test
    fun report_notifications() {
        for (source in listOf(Source.CATCH_AFTER_FLOW_ON, Source.PER_MESSAGE)) {
            for (whileWaiting in listOf(true, false)) measure(Row(source, whileWaiting))
        }
    }

    /** The work moved into onEach before catch, with and without flowOn after it. Asserts nothing. */
    @Test
    fun report_onEach() {
        for (collector in listOf(Collector.ON_EACH, Collector.ON_EACH_FLOW_ON)) {
            for (whileWaiting in listOf(true, false)) measure(Row(Source.CATCH_AFTER_FLOW_ON, whileWaiting, collector))
        }
    }

    /**
     * collectLatest instead of collect, catch still after flowOn. A poll, or a text, arrives while the
     * observer waits after posting the first text. Counts crashes, and runs in which the first text was
     * posted twice. Plain collect with a text arriving is the comparison: Room reads the newer list before
     * the first text is marked as notified, so it holds the first text either way. Asserts nothing.
     */
    @Test
    fun report_collectLatest() {
        for (arriving in listOf("POLL", "TEXT")) {
            measure(Row(Source.CATCH_AFTER_FLOW_ON, whileWaiting = true, collector = Collector.COLLECT_LATEST, arriving = arriving))
        }
        measure(Row(Source.CATCH_AFTER_FLOW_ON, whileWaiting = true, collector = Collector.COLLECT, arriving = "TEXT"))
    }

    /**
     * With catch before flowOn nothing crashes. The repository's catch running in every run also proves
     * that the mapper really threw.
     */
    @Test
    fun catchBeforeFlowOn_neverCrashes() {
        for (whileWaiting in listOf(true, false)) {
            val result = measure(Row(Source.CATCH_BEFORE_FLOW_ON, whileWaiting))
            assertEquals("crashes in: ${result.summary()}", 0, result.crashes)
            assertEquals("the mapper did not throw in every run: ${result.summary()}", result.runs, result.repositoryLogged)
        }
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
        /** Wire's NOTIFICATION_UPDATE_DELAY. */
        const val WAIT_MS = 100L
    }
}
