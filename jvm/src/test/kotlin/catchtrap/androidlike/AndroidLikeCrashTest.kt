package catchtrap.androidlike

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.UnknownHostException

/**
 * kotlinx.coroutines #4159 in the shape of an Android feature.
 *
 * "The app crashed" means: an exception reached the default uncaught-exception handler, which is
 * where a failed `viewModelScope.launch` ends up and where Android prints FATAL EXCEPTION.
 *
 * The first three tests assert that the bug EXISTS. They pass on kotlinx.coroutines 1.11.0 and
 * should start failing on the release that fixes the issue.
 */
class AndroidLikeCrashTest {

    private val android = FakeAndroid()
    private val dao = ProfileDao()
    private val cached = ProfileUiState.Content(dao.cachedProfile())

    @After
    fun tearDown() = android.close()

    private fun viewModel(style: NetworkStyle) = ProfileViewModel(
        repository = ProfileRepository(dao, ProfileApi(style), Dispatchers.IO),
        dao = dao,
        viewModelScope = android.newViewModelScope(),
    )

    /**
     * No `first()`, nobody leaves the screen. The collector is simply suspended in `dao.save()`
     * when the network call fails, and the error skips `catch`.
     */
    @Test
    fun collectorSuspended_whenUpstreamFails_errorSkipsCatch_andCrashes() {
        val viewModel = viewModel(NetworkStyle.SUSPENDING)

        android.onMainThread { viewModel.observeAndPersist().join() }

        assertTrue("expected a crash", android.crashes.single() is UnknownHostException)
        // The catch block never ran: the screen was never given the Error state.
        assertEquals(listOf<ProfileUiState>(cached), viewModel.shown)
    }

    /** `catch { emit(placeholder) }.first()`: the placeholder never arrives, the exception does. */
    @Test
    fun loadOnce_withBlockingNetworkCall_errorSkipsCatch_andCrashes() {
        val viewModel = viewModel(NetworkStyle.BLOCKING)

        android.onMainThread { viewModel.loadOnce().join() }

        assertTrue("expected a crash", android.crashes.single() is UnknownHostException)
        assertEquals(emptyList<ProfileUiState>(), viewModel.shown)
    }

    /** The user presses back while the collector is working; the app crashes after the screen is gone. */
    @Test
    fun userLeaves_whileCollectorWorks_errorSkipsCatch_andCrashes() {
        val viewModel = viewModel(NetworkStyle.BLOCKING)

        android.onMainThread {
            val job = viewModel.observeAndPersist()
            delay(2)
            viewModel.onCleared()
            job.join()
        }

        assertTrue("expected a crash", android.crashes.single() is UnknownHostException)
    }

    /** With `catch` before `flowOn`, nothing crashes, and the cached profile is shown whenever the user stays. */
    @Test
    fun fixedRepository_neverCrashes_andShowsTheCachedProfileWhenTheUserStays() {
        for (action in UserAction.entries) {
            for (style in NetworkStyle.entries) {
                val row = runAndroidLikeRow(android, action, style, fixed = true, runs = 40)
                assertEquals("crashes in: ${row.summary()}", 0, row.crashes)
                // If the user leaves after 2 ms, the screen may close before anything arrives.
                val userStays = action != UserAction.OBSERVE_THEN_LEAVE &&
                    action != UserAction.OBSERVE_PERSIST_THEN_LEAVE
                if (userStays) {
                    assertEquals("cached profile missing in: ${row.summary()}", row.runs, row.cachedShown)
                }
            }
        }
    }

    /** Prints the full report: `./gradlew test --tests '*AndroidLikeCrashTest.printReport'`. */
    @Test
    fun printReport() {
        runAndroidLikeReport(runs = 100)
    }
}
