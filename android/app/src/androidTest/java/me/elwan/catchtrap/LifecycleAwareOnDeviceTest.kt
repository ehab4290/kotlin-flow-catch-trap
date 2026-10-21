package me.elwan.catchtrap

import android.util.Log
import androidx.lifecycle.Lifecycle
import androidx.test.core.app.ActivityScenario
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import me.elwan.catchtrap.lifecycle.NetworkCall
import me.elwan.catchtrap.lifecycle.Collector
import me.elwan.catchtrap.lifecycle.LifecycleAwareActivity
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Does lifecycle-aware collection change the trap? catch is after flowOn in every row.
 *
 *  - On screen: the network call fails while the app is visible.
 *  - Background trip: the app goes to the background (a real onStop) while the call is in progress,
 *    then comes back. The call nearly always fails before onStop is reached; the row prints how
 *    often it failed in the background.
 *
 * The only thing replaced is the handler that would kill the process, so the runs can be counted.
 *
 *     adb shell am instrument -w -e runs 50 -e class me.elwan.catchtrap.LifecycleAwareOnDeviceTest \
 *         me.elwan.catchtrap.test/androidx.test.runner.AndroidJUnitRunner
 *     adb logcat -s CatchTrap
 */
@RunWith(AndroidJUnit4::class)
class LifecycleAwareOnDeviceTest {

    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val act = LifecycleAwareActivity

    private fun waitUntil(ms: Long, done: () -> Boolean) {
        val deadline = System.currentTimeMillis() + ms
        while (System.currentTimeMillis() < deadline && !done()) Thread.sleep(20)
    }

    /** What the user does while the network call is in progress. */
    enum class Trip(val label: String) {
        NONE("on screen"),
        BACKGROUND("background trip"),
        LEAVE("user leaves (back), ViewModel cleared"),
    }

    private fun measure(collector: Collector, call: NetworkCall, backgroundTrip: Boolean) =
        measure(collector, call, if (backgroundTrip) Trip.BACKGROUND else Trip.NONE)

    private fun measure(collector: Collector, call: NetworkCall, trip: Trip, callMs: Long = LifecycleAwareActivity.CALL_MS) {
        val backgroundTrip = trip == Trip.BACKGROUND
        val uncaught = CopyOnWriteArrayList<Throwable>()
        val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
        var crashes = 0
        var crashesInBackground = 0
        var fallbackShown = 0
        var catchRan = 0
        var callsStarted = 0
        var firstCallEndedInBackground = 0
        var cachedShown = 0
        var clearedDuringCall = 0
        var late = 0
        var sample: Throwable? = null
        try {
            repeat(RUNS) {
                uncaught.clear()
                act.reset()
                val scenario = ActivityScenario.launch<LifecycleAwareActivity>(act.intent(context, collector, call, callMs))
                if (trip == Trip.LEAVE) {
                    waitUntil(3_000) { act.CACHED in act.shown || uncaught.isNotEmpty() }
                    Thread.sleep(100)                 // the call is in progress
                    scenario.close()                  // back: the activity is destroyed, onCleared() cancels viewModelScope
                    if (act.viewModelCleared && act.callsStarted.get() > 0) clearedDuringCall++
                    Thread.sleep(act.CALL_MS + 500)   // the call fails after the screen is gone
                } else scenario.use { scenario ->
                    if (backgroundTrip) {
                        waitUntil(3_000) { act.CACHED in act.shown || uncaught.isNotEmpty() }
                        Thread.sleep(100)                              // the call is in progress
                        scenario.moveToState(Lifecycle.State.CREATED)  // onStop: the app is in the background
                        Thread.sleep(act.CALL_MS + 500)               // wait for the call to fail (usually before onStop was reached)
                        if (act.callsEndedInBackground.get() > 0) firstCallEndedInBackground++
                        if (uncaught.isNotEmpty()) crashesInBackground++
                        else {
                            scenario.moveToState(Lifecycle.State.RESUMED) // the user comes back
                            waitUntil(act.CALL_MS + 3_000) { act.FALLBACK in act.shown || uncaught.isNotEmpty() }
                        }
                    } else {
                        waitUntil(act.CALL_MS + 3_000) { act.FALLBACK in act.shown || uncaught.isNotEmpty() }
                    }
                    Thread.sleep(200) // let a late crash land before the activity closes
                }
                if (uncaught.isNotEmpty()) {
                    crashes++
                    if (sample == null) sample = uncaught.first()
                }
                if (act.FALLBACK in act.shown) fallbackShown++
                if (act.CACHED in act.shown) cachedShown++
                catchRan += act.catchRan.get()
                callsStarted += act.callsStarted.get()
                late += act.late.get()
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(systemHandler)
        }
        val summary = buildString {
            append("[").append(trip.label).append(", ")
            append(call.name.lowercase()).append(" call, fails after ${callMs} ms] ").append(collector.label)
            append(" | crashed $crashes/$RUNS")
            if (backgroundTrip) append(" ($crashesInBackground in the background)")
            append(", cached shown $cachedShown/$RUNS, fallback shown $fallbackShown/$RUNS, catch ran $catchRan, calls started $callsStarted")
            if (trip == Trip.LEAVE) append(", ViewModel cleared during the call $clearedDuringCall/$RUNS")
            if (backgroundTrip) append(", first call failed in the background in $firstCallEndedInBackground/$RUNS")
            if (late > 0) append(", late callbacks from an earlier run $late (not counted)")
            sample?.let { append(" | ${it.javaClass.simpleName}: ${it.message}") }
        }
        Log.i(TAG, summary)
    }

    /** Counts every row. Asserts nothing, so one surprising row doesn't stop the others. */
    @Test
    fun report_lifecycleAware() {
        // The error arrives while the screen is visible.
        measure(Collector.STATE_WITH_LIFECYCLE, NetworkCall.SUSPENDING, backgroundTrip = false)
        measure(Collector.REPEAT_SNACKBAR, NetworkCall.SUSPENDING, backgroundTrip = false)
        measure(Collector.LAUNCHED_EFFECT_SNACKBAR, NetworkCall.SUSPENDING, backgroundTrip = false)
        // The app goes to the background while the call is in progress, then comes back.
        for (call in NetworkCall.entries) {
            measure(Collector.STATE_WITH_LIFECYCLE, call, backgroundTrip = true)
            measure(Collector.REPEAT_STATE, call, backgroundTrip = true)
            measure(Collector.COLLECT_AS_STATE, call, backgroundTrip = true)
        }
    }

    /**
     * The article's profile ViewModel: map, catch, stateIn(WhileSubscribed(5_000)), read with
     * collectAsStateWithLifecycle(). Then the safer way, with catch before flowOn.
     */
    @Test
    fun report_stateInViewModel() {
        for (collector in listOf(Collector.STATE_IN, Collector.STATE_IN_SAFER)) {
            for (call in NetworkCall.entries) {
                for (trip in Trip.entries) measure(collector, call, trip)
            }
            // Offline at a cold start: the call fails at once, while the first frame is still being drawn.
            // Does the screen draw the cached profile before the error state replaces it?
            measure(collector, NetworkCall.BLOCKING, Trip.NONE, callMs = 0)
        }
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
    }
}
