package me.elwan.catchtrap

import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import me.elwan.catchtrap.data.ProfileApi
import me.elwan.catchtrap.data.ProfileDatabase
import me.elwan.catchtrap.data.ProfileRepository
import me.elwan.catchtrap.measure.MeasuredRow
import me.elwan.catchtrap.measure.Measurement
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

/**
 * kotlinx.coroutines #4159 on a device or emulator, with nothing replaced except the handler that
 * would kill the process: a real ViewModel on the real main thread, a Room database file, and
 * Retrofit + OkHttp failing a real DNS lookup for a host that cannot exist.
 *
 * The counts are printed to Logcat under the tag CatchTrap:
 *
 *     adb logcat -s CatchTrap
 */
@RunWith(AndroidJUnit4::class)
class CatchTrapOnDeviceTest {

    private lateinit var database: ProfileDatabase
    private lateinit var repository: ProfileRepository

    @Before
    fun setUp() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        context.deleteDatabase(DATABASE_NAME)
        database = ProfileDatabase.create(context, DATABASE_NAME)
        val apis = ConcurrentHashMap<Host, ProfileApi>()
        repository = ProfileRepository(
            dao = database.profileDao(),
            apiFor = { host -> apis.getOrPut(host) { ProfileApi.create(host.baseUrl) } },
        )
    }

    @After
    fun tearDown() {
        database.close()
    }

    private fun measure(scenario: Scenario, callStyle: CallStyle): MeasuredRow {
        val setup = NetworkSetup(Host.UNRESOLVABLE, callStyle)
        val row = runBlocking { Measurement.measure(repository, scenario, setup, RUNS) }
        Log.i(TAG, row.summary())
        return row
    }

    /**
     * `catch { emit(placeholder) }.first()` with a blocking network call. The call cannot be cancelled,
     * so it fails after `first()` already has the cached value, and the error skips `catch`.
     *
     * This asserts that the bug exists. It should start failing on the release that fixes #4159.
     */
    @Test
    fun loadOnce_withBlockingCall_errorSkipsCatch() {
        val row = measure(Scenario.LOAD_ONCE, CallStyle.BLOCKING)

        assertTrue("expected at least one crash: ${row.summary()}", row.crashes > 0)
        assertTrue("expected a network error, got ${row.sampleCrash}", row.sampleCrash is IOException)
    }

    /** With `catch` before `flowOn`, nothing crashes and the cached profile always arrives. */
    @Test
    fun fixedRepository_neverCrashes_andAlwaysShowsTheCachedProfile() {
        for (scenario in Scenario.entries.filter { it.fixed }) {
            for (callStyle in CallStyle.entries) {
                val row = measure(scenario, callStyle)
                assertEquals("crashes in: ${row.summary()}", 0, row.crashes)
                assertEquals("cached profile missing in: ${row.summary()}", row.runs, row.cachedShown)
            }
        }
    }

    /**
     * Counts every vulnerable scenario with both call styles and asserts nothing: how often the
     * collector is still suspended when the network fails depends on the device.
     */
    @Test
    fun report_vulnerableScenarios() {
        for (scenario in Scenario.entries.filter { !it.fixed }) {
            for (callStyle in CallStyle.entries) {
                measure(scenario, callStyle)
            }
        }
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
        const val DATABASE_NAME = "profile-test.db"
    }
}
