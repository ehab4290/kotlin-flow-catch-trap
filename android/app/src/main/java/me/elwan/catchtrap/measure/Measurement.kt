package me.elwan.catchtrap.measure

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import me.elwan.catchtrap.NetworkSetup
import me.elwan.catchtrap.Profile
import me.elwan.catchtrap.ProfileUiState
import me.elwan.catchtrap.Scenario
import me.elwan.catchtrap.data.ProfileRepository
import me.elwan.catchtrap.ui.ProfileViewModel
import java.util.concurrent.CopyOnWriteArrayList

data class MeasuredRow(
    val scenario: Scenario,
    val setup: NetworkSetup,
    val runs: Int,
    /** Runs in which an exception reached the default uncaught-exception handler. */
    val crashes: Int,
    /** Runs in which the screen was given the cached profile. */
    val cachedShown: Int,
    /** Runs that did not finish within the timeout. */
    val timedOut: Int,
    val sampleCrash: Throwable?,
) {
    fun summary(): String = buildString {
        append(if (scenario.fixed) "[catch before flowOn] " else "[catch after flowOn]  ")
        append(scenario.title.substringBefore(':'))
        append(" | ").append(setup.callStyle.label)
        append(" | crashed ").append(crashes).append('/').append(runs)
        append(", cached shown ").append(cachedShown).append('/').append(runs)
        if (timedOut > 0) append(", timed out ").append(timedOut)
        sampleCrash?.let { append(" | ").append(it.javaClass.simpleName) }
    }
}

object Measurement {

    /**
     * Runs [scenario] [runs] times: a real ViewModel on the real main thread, the real Room database
     * and the real network stack.
     *
     * The one thing it changes is what happens after the crash. While it runs, the default
     * uncaught-exception handler (the one that prints FATAL EXCEPTION and kills the process) is
     * replaced by one that records the exception, so the process survives and the runs can be counted.
     */
    suspend fun measure(
        repository: ProfileRepository,
        scenario: Scenario,
        setup: NetworkSetup,
        runs: Int,
    ): MeasuredRow = withContext(Dispatchers.Main.immediate) {
        val uncaught = CopyOnWriteArrayList<Throwable>()
        val previousHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }

        var crashes = 0
        var cachedShown = 0
        var timedOut = 0
        var sample: Throwable? = null
        try {
            repeat(runs) {
                uncaught.clear()
                val viewModel = ProfileViewModel(repository)
                val job = viewModel.run(scenario, setup)
                if (withTimeoutOrNull(30_000) { job.join() } == null) {
                    timedOut++
                    job.cancel()
                }
                if (uncaught.isNotEmpty()) {
                    crashes++
                    if (sample == null) sample = uncaught.first()
                }
                val sawCached = viewModel.states.value.any {
                    it is ProfileUiState.Content && it.profile.origin == Profile.Origin.CACHE
                }
                if (sawCached) cachedShown++
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previousHandler)
        }
        MeasuredRow(scenario, setup, runs, crashes, cachedShown, timedOut, sample)
    }
}
