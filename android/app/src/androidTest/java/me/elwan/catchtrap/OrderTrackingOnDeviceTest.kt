package me.elwan.catchtrap

import android.util.Log
import androidx.lifecycle.viewModelScope
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import me.elwan.catchtrap.orders.MessageStyle
import me.elwan.catchtrap.orders.OrderDatabase
import me.elwan.catchtrap.orders.OrderEntity
import me.elwan.catchtrap.orders.OrderRepository
import me.elwan.catchtrap.orders.OrderViewModel
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Order tracking with real parts: a Room database, a Room Flow, a real ViewModel on the real
 * viewModelScope, and the real main thread. The test plays the sync worker: it writes a status this
 * app version does not know (handled), then ON_THE_WAY with no driver, as for an order handed to a
 * partner courier. The repository's mapper requires a driver for ON_THE_WAY and throws.
 *
 * [report_pileUp] sends many updates at once instead, then the one without a driver, through the same
 * repository chain, to find where each message style starts to wait: Room merges quick writes, so it
 * can't produce a pile-up. The failing update is sent only once the ViewModel has received all the
 * others (or after [GATE_MS]), so an update still in the repository's own flowOn queue can't decide the
 * row: what's measured is room in the handoff to the UI. Each pile-up row prints how many updates the
 * ViewModel and the UI actually got.
 *
 * The only thing replaced is the handler that would kill the process, so the runs can be counted.
 *
 *     ./gradlew connectedDebugAndroidTest
 *     adb logcat -s CatchTrap
 */
@RunWith(AndroidJUnit4::class)
class OrderTrackingOnDeviceTest {

    private val context = InstrumentationRegistry.getInstrumentation().targetContext

    private data class Row(
        val catchInRepository: Boolean,
        val style: MessageStyle,
        val uiOnScreen: Boolean,
        /** 0: two status writes to Room. Otherwise this many updates at once, then the one that throws. */
        val updatesAtOnce: Int = 0,
    ) {
        val label: String
            get() = buildString {
                append(if (catchInRepository) "[catch in repository, before flowOn] " else "[catch in ViewModel, after flowOn] ")
                append(style.label)
                append(if (uiOnScreen) ", UI showing a snackbar" else ", app in the background")
                if (updatesAtOnce > 0) append(", $updatesAtOnce updates at once")
            }
    }

    private data class Result(
        val row: Row,
        val runs: Int,
        val crashes: Int,
        val viewModelCatchRan: Int,
        /** Runs in which the repository's catch ran, i.e. the mapper threw. Only the fixed rows have one. */
        val repositoryCatchRan: Int,
        val sampleCrash: Throwable?,
        /** Pile-up rows only: the fewest and most updates the ViewModel and the UI received in a run. */
        val viewModelGot: IntRange? = null,
        val uiGot: IntRange? = null,
    ) {
        fun summary() = "${row.label} | crashed $crashes/$runs, ViewModel's catch ran $viewModelCatchRan, " +
            "repository's catch ran $repositoryCatchRan" +
            (viewModelGot?.let { ", ViewModel received ${it.first} to ${it.last} of ${row.updatesAtOnce}" } ?: "") +
            (uiGot?.let { ", UI received ${it.first} to ${it.last}" } ?: "") +
            (sampleCrash?.let { " | ${it.javaClass.simpleName}" } ?: "")
    }

    private fun measure(row: Row, runs: Int = RUNS): Result = runBlocking {
        withContext(Dispatchers.Main.immediate) {
            val uncaught = CopyOnWriteArrayList<Throwable>()
            val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
            Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
            var crashes = 0
            var catchRan = 0
            var repositoryCatchRan = 0
            var sample: Throwable? = null
            var vmMin = Int.MAX_VALUE; var vmMax = 0; var uiMin = Int.MAX_VALUE; var uiMax = 0
            try {
                repeat(runs) {
                    uncaught.clear()
                    val database = OrderDatabase.inMemory(context)
                    val dao = database.orderDao()
                    dao.upsert(OrderEntity("42", "PREPARING"))
                    val repository = OrderRepository(dao)
                    val go = CompletableDeferred<Unit>()
                    val viewModelGot = AtomicInteger()
                    val uiGot = AtomicInteger()
                    val orders = when {
                        row.updatesAtOnce > 0 -> repository.orders(flow {
                            go.await()                                                // once the UI is collecting
                            repeat(row.updatesAtOnce) { emit(OrderEntity("42", "PREPARING")) }
                            // Wait until the ViewModel has received them all, so none is still in flowOn's queue.
                            withTimeoutOrNull(GATE_MS) { while (viewModelGot.get() < row.updatesAtOnce) delay(1) }
                            emit(OrderEntity("42", "ON_THE_WAY", driverName = null))  // the mapper throws
                        })
                        row.catchInRepository -> repository.ordersCaughtBeforeFlowOn("42")
                        else -> repository.orders("42")
                    }
                    val viewModel = OrderViewModel(orders, row.style, onReceived = { viewModelGot.incrementAndGet() })

                    val ui = CoroutineScope(SupervisorJob() + Dispatchers.Main)
                    if (row.uiOnScreen) ui.launch {
                        val updates = viewModel.state ?: viewModel.messages   // the StateFlow rows have no messages
                        updates.collect { uiGot.incrementAndGet(); delay(SNACKBAR_MS) } // snackbarHostState.showSnackbar(it)
                    }

                    delay(100)
                    if (row.updatesAtOnce > 0) {
                        go.complete(Unit)
                        delay(GATE_MS + 800)
                    } else {
                        dao.upsert(OrderEntity("42", "AT_PICKUP_LOCKER"))                 // unknown to this app version: handled
                        delay(100)
                        dao.upsert(OrderEntity("42", "ON_THE_WAY", driverName = null))    // partner courier: no driver
                        delay(700)
                    }

                    if (uncaught.isNotEmpty()) {
                        crashes++
                        if (sample == null) sample = uncaught.first()
                    }
                    catchRan += viewModel.fallbackRan.get()
                    if (repository.mapperFailures.get() > 0) repositoryCatchRan++
                    if (row.updatesAtOnce > 0) {
                        vmMin = minOf(vmMin, viewModelGot.get()); vmMax = maxOf(vmMax, viewModelGot.get())
                        uiMin = minOf(uiMin, uiGot.get()); uiMax = maxOf(uiMax, uiGot.get())
                    }
                    // Wait for both scopes to finish before closing the database, so no query is still running.
                    withTimeoutOrNull(2_000) {
                        ui.coroutineContext[Job]!!.cancelAndJoin()
                        viewModel.viewModelScope.coroutineContext[Job]!!.cancelAndJoin()
                    }
                    database.close()
                }
            } finally {
                Thread.setDefaultUncaughtExceptionHandler(systemHandler)
            }
            val pileUp = row.updatesAtOnce > 0
            Result(
                row, runs, crashes, catchRan, repositoryCatchRan, sample,
                viewModelGot = if (pileUp) vmMin..vmMax else null,
                uiGot = if (pileUp && row.uiOnScreen) uiMin..uiMax else null,
            ).also { Log.i(TAG, it.summary()) }
        }
    }

    /** Counts every combination. Asserts nothing: how often the ViewModel is suspended depends on the device. */
    @Test
    fun report_orderTracking() {
        val rows = MessageStyle.entries.flatMap { style ->
            listOf(false, true).map { uiOnScreen -> Row(catchInRepository = false, style, uiOnScreen) }
        }
        for (row in rows) measure(row)
    }

    /**
     * Many updates at once, catch after flowOn. Channel(BUFFERED) and shareIn hold 64 messages: 63 updates
     * fit, 66 don't, so the ViewModel's collector waits. The StateFlow rows keep only the latest value,
     * so 100 updates never make it wait. Asserts nothing.
     */
    @Test
    fun report_pileUp() {
        val rows = listOf(MessageStyle.BUFFERED_CHANNEL, MessageStyle.SHARE_IN).flatMap { style ->
            listOf(63, 66).flatMap { n -> listOf(true, false).map { ui -> Row(false, style, ui, updatesAtOnce = n) } }
        } + listOf(MessageStyle.STATE_IN, MessageStyle.MUTABLE_STATE).flatMap { style ->
            listOf(true, false).map { ui -> Row(false, style, ui, updatesAtOnce = 100) }
        }
        for (row in rows) measure(row)
    }

    /**
     * With catch before flowOn nothing crashes. The repository's catch running in every run also proves
     * that the mapper really threw.
     */
    @Test
    fun catchBeforeFlowOn_neverCrashes() {
        for (uiOnScreen in listOf(false, true)) {
            val result = measure(Row(catchInRepository = true, MessageStyle.RENDEZVOUS_CHANNEL, uiOnScreen))
            assertEquals("crashes in: ${result.summary()}", 0, result.crashes)
            assertEquals("the mapper did not throw in every run: ${result.summary()}", result.runs, result.repositoryCatchRan)
        }
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
        const val SNACKBAR_MS = 1_000L
        /** Pile-up rows: how long the source waits for the ViewModel to receive every update. */
        const val GATE_MS = 1_000L
    }
}
