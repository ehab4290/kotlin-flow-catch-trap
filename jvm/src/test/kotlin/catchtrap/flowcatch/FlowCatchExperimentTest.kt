package catchtrap.flowcatch

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Runs the full grid and prints the report.
 *
 * The buggy scenarios depend on timing, so they are reported, not asserted.
 * The two fix scenarios must be correct in every single run.
 */
class FlowCatchExperimentTest {

    private val main = newFakeMainDispatcher()

    @After
    fun tearDown() = main.close()

    @Test
    fun fixes_are_always_correct() {
        println("kotlinx.coroutines ${coroutinesVersion()}, 200 runs per row")
        val results = runBlocking(main) { runGrid(ProfileRepository(Dispatchers.IO), runs = 200) }

        val brokenFixes = results.filter { it.scenario.isFix && it.bugCount > 0 }
        assertEquals(
            "catch before flowOn must never lose the value or leak the error",
            emptyList<ScenarioResult>(),
            brokenFixes,
        )
    }
}
