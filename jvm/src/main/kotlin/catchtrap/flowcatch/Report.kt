package catchtrap.flowcatch

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

/** The kotlinx.coroutines version actually on the classpath. */
fun coroutinesVersion(): String =
    Dispatchers::class.java.`package`?.implementationVersion ?: "unknown"

/** A single thread that plays the role of Android's main thread. */
fun newFakeMainDispatcher(threadName: String = "fake-main"): ExecutorCoroutineDispatcher =
    Executors.newSingleThreadExecutor { r -> Thread(r, threadName).apply { isDaemon = true } }
        .asCoroutineDispatcher()

/**
 * Runs every [Scenario] for every combination of timings, printing each row as it finishes.
 * Call it on the fake main dispatcher.
 */
suspend fun runGrid(
    repo: ProfileRepository,
    runs: Int,
    failAfterMs: List<Long> = listOf(0, 2, 10),
    mainBusyMs: List<Long> = listOf(0, 16),
    print: (String) -> Unit = ::println,
): List<ScenarioResult> {
    val results = mutableListOf<ScenarioResult>()
    for (fail in failAfterMs) {
        for (busy in mainBusyMs) {
            print("")
            for (scenario in Scenario.entries) {
                val result = runScenario(scenario, repo, RunParams(runs, fail, busy))
                print(result.summary())
                results += result
            }
        }
    }
    return results
}
