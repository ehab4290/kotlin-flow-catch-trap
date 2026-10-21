package me.elwan.catchtrap

import android.os.Looper
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlin.coroutines.ContinuationInterceptor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Does `.flowOn(Dispatchers.Main)` inside `viewModelScope` create a channel, even though everything
 * stays on the main thread?
 *
 * viewModelScope runs on Dispatchers.Main.immediate. flowOn skips the channel only when the
 * dispatcher object is equal (ChannelFlowOperator.collect), and Android's HandlerContext.equals
 * also compares the immediate flag. So Main and Main.immediate should count as different.
 *
 * Each run: the source emits, waits 20 ms, then throws. The collector waits 100 ms per value,
 * so it is busy when the source fails. With a channel, catch is skipped and the app would crash.
 *
 *     ./gradlew connectedDebugAndroidTest -Pandroid.testInstrumentationRunnerArguments.class=me.elwan.catchtrap.FlowOnMainImmediateTest
 *     adb logcat -s CatchTrap
 */
@RunWith(AndroidJUnit4::class)
class FlowOnMainImmediateTest {

    class ProbeViewModel : ViewModel()

    private data class Result(
        val label: String,
        val runs: Int,
        val crashes: Int,
        val fallbackShown: Int,
        /** Runs in which emit() returned before the collector had finished with the value: a channel. */
        val ranAhead: Int,
        val allOnMainThread: Boolean,
        val scopeDispatcher: String,
    ) {
        fun summary() = "[$label] scope=$scopeDispatcher | crashed $crashes/$runs, catch fallback $fallbackShown/$runs, " +
            "emit returned early (channel) $ranAhead/$runs, all on main thread: $allOnMainThread"
    }

    private fun measure(label: String, runs: Int = RUNS, upstream: (Flow<String>) -> Flow<String>): Result {
        val uncaught = CopyOnWriteArrayList<Throwable>()
        val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
        var crashes = 0
        var fallback = 0
        var ranAhead = 0
        var allOnMain = true
        var scopeDispatcher = "?"
        try {
            repeat(runs) {
                uncaught.clear()
                val vm = ProbeViewModel()
                scopeDispatcher = vm.viewModelScope.coroutineContext[ContinuationInterceptor].toString()
                val onMain = AtomicBoolean(true)
                val fallbackShown = AtomicBoolean(false)
                val emitReturnedEarly = AtomicBoolean(false)
                val finishedValues = AtomicInteger(0)
                val done = CountDownLatch(1)

                val source = flow {
                    if (Looper.myLooper() != Looper.getMainLooper()) onMain.set(false)
                    emit("PREPARING")
                    // Without a channel, emit() returns only after the collector has finished with the value.
                    if (finishedValues.get() == 0) emitReturnedEarly.set(true)
                    delay(20)
                    throw IllegalStateException("source failed")
                }

                val job = vm.viewModelScope.launch {
                    upstream(source)
                        .catch { emit("UNAVAILABLE") }
                        .collect { value ->
                            if (Looper.myLooper() != Looper.getMainLooper()) onMain.set(false)
                            if (value == "UNAVAILABLE") fallbackShown.set(true)
                            delay(100) // the collector is busy, like showSnackbar()
                            finishedValues.incrementAndGet()
                        }
                }
                job.invokeOnCompletion { done.countDown() }
                assertTrue("run $it of [$label] did not finish", done.await(5, TimeUnit.SECONDS))
                Thread.sleep(100) // let a crash reach the handler
                vm.viewModelScope.cancel()

                if (uncaught.isNotEmpty()) crashes++
                if (fallbackShown.get()) fallback++
                if (emitReturnedEarly.get()) ranAhead++
                if (!onMain.get()) allOnMain = false
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(systemHandler)
        }
        return Result(label, runs, crashes, fallback, ranAhead, allOnMain, scopeDispatcher).also { Log.i(TAG, it.summary()) }
    }

    @Test
    fun flowOnMain_insideViewModelScope_createsAChannel() {
        val r = measure("flowOn(Dispatchers.Main) in viewModelScope") { it.flowOn(Dispatchers.Main) }
        assertTrue(r.summary(), r.allOnMainThread)
        assertEquals(r.summary(), r.runs, r.ranAhead)
        assertEquals(r.summary(), r.runs, r.crashes)
        assertEquals(r.summary(), 0, r.fallbackShown)
    }

    @Test
    fun flowOnMainImmediate_insideViewModelScope_needsNoChannel() {
        val r = measure("flowOn(Dispatchers.Main.immediate) in viewModelScope") { it.flowOn(Dispatchers.Main.immediate) }
        assertTrue(r.summary(), r.allOnMainThread)
        assertEquals(r.summary(), 0, r.ranAhead)
        assertEquals(r.summary(), 0, r.crashes)
        assertEquals(r.summary(), r.runs, r.fallbackShown)
    }

    @Test
    fun noFlowOn_control() {
        val r = measure("no flowOn in viewModelScope") { it }
        assertTrue(r.summary(), r.allOnMainThread)
        assertEquals(r.summary(), 0, r.ranAhead)
        assertEquals(r.summary(), 0, r.crashes)
        assertEquals(r.summary(), r.runs, r.fallbackShown)
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
    }
}
