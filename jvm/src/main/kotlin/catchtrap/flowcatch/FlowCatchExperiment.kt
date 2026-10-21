@file:OptIn(ExperimentalCoroutinesApi::class)

package catchtrap.flowcatch

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

/**
 * Experiments for kotlinx.coroutines issue #4159 ("Flow.catch skips upstream exceptions")
 * and the related loss of values emitted just before an upstream error.
 *
 * https://github.com/Kotlin/kotlinx.coroutines/issues/4159
 */

/** Thrown by the simulated network refresh, like UnknownHostException when the device is offline. */
class OfflineError : RuntimeException("Unable to resolve host (simulated offline)")

const val CACHED = "cached profile"
const val ERROR_STATE = "error state"

/**
 * A repository in the shape used by the official Android guide (developer.android.com/kotlin/flow):
 * `flowOn` inside the repository, `catch` further downstream.
 */
class ProfileRepository(private val io: CoroutineDispatcher) {

    /** The raw source: the cached row from the database, then a network refresh that fails. */
    fun source(failAfterMs: Long): Flow<String> = flow {
        emit(CACHED)
        if (failAfterMs > 0) delay(failAfterMs)
        throw OfflineError()
    }

    /** Vulnerable shape: the error has to cross the flowOn boundary before anything catches it. */
    fun profile(failAfterMs: Long): Flow<String> = source(failAfterMs).flowOn(io)

    /** The fix: turn the error into a value before crossing the flowOn boundary. */
    fun profileSafe(failAfterMs: Long): Flow<String> =
        source(failAfterMs).catch { emit(ERROR_STATE) }.flowOn(io)
}

enum class Outcome(val label: String, val isBug: Boolean) {
    OK("cached value delivered", isBug = false),
    VALUE_LOST("cached value lost", isBug = true),
    ESCAPED("error escaped catch/retry", isBug = true),
}

enum class Scenario(val title: String, val code: String, val isFix: Boolean = false) {

    COLLECT_CATCH_AFTER_FLOWON(
        title = "collect, catch after flowOn",
        code = "source.flowOn(IO).catch { emit(error) }.collect { }",
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            collectOutcome(repo.profile(failAfterMs).catch { emit(ERROR_STATE) })
    },

    COLLECT_CATCH_BEFORE_FLOWON(
        title = "collect, catch before flowOn (fix)",
        code = "source.catch { emit(error) }.flowOn(IO).collect { }",
        isFix = true,
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            collectOutcome(repo.profileSafe(failAfterMs))
    },

    FIRST_CATCH_AFTER_FLOWON(
        title = "first(), catch after flowOn",
        code = "source.flowOn(IO).catch { emit(error) }.first()",
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            firstOutcome { repo.profile(failAfterMs).catch { emit(ERROR_STATE) }.first() }
    },

    FIRST_CATCH_BEFORE_FLOWON(
        title = "first(), catch before flowOn (fix)",
        code = "source.catch { emit(error) }.flowOn(IO).first()",
        isFix = true,
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            firstOutcome { repo.profileSafe(failAfterMs).first() }
    },

    RETRY_AFTER_FLOWON(
        title = "first(), retry(3) after flowOn",
        code = "source.flowOn(IO).retry(3).first()",
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            firstOutcome { repo.profile(failAfterMs).retry(3).first() }
    },

    BUFFER_THEN_CATCH(
        title = "collect, buffer() then catch",
        code = "source.buffer().catch { emit(error) }.collect { }",
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            collectOutcome(repo.source(failAfterMs).buffer().catch { emit(ERROR_STATE) })
    },

    FLATMAPLATEST_TAKE1(
        title = "take(1), flatMapLatest then catch",
        code = "flowOf(Unit).flatMapLatest { source }.catch { emit(error) }.take(1)",
    ) {
        override suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long) =
            collectOutcome(
                flowOf(Unit).flatMapLatest { repo.source(failAfterMs) }.catch { emit(ERROR_STATE) }.take(1)
            )
    };

    abstract suspend fun runOnce(repo: ProfileRepository, failAfterMs: Long): Outcome
}

private suspend fun collectOutcome(flow: Flow<String>): Outcome {
    val seen = ArrayList<String>(2)
    try {
        flow.collect { seen += it }
    } catch (e: OfflineError) {
        return Outcome.ESCAPED
    }
    return if (CACHED in seen) Outcome.OK else Outcome.VALUE_LOST
}

private suspend fun firstOutcome(block: suspend () -> String): Outcome =
    try {
        if (block() == CACHED) Outcome.OK else Outcome.VALUE_LOST
    } catch (e: OfflineError) {
        Outcome.ESCAPED
    }

/**
 * @property failAfterMs how long the simulated network refresh takes before failing.
 * @property mainBusyMs how long the main thread stays busy right after collection starts
 *   (a frame being drawn, a layout being inflated).
 */
data class RunParams(val runs: Int, val failAfterMs: Long, val mainBusyMs: Long)

data class ScenarioResult(val scenario: Scenario, val params: RunParams, val counts: Map<Outcome, Int>) {
    val bugCount: Int get() = counts.filterKeys { it.isBug }.values.sum()

    fun summary(): String = String.format(
        "%-38s fail %3d ms, main busy %2d ms | ok %4d  lost %4d  escaped %4d",
        scenario.title,
        params.failAfterMs,
        params.mainBusyMs,
        counts[Outcome.OK] ?: 0,
        counts[Outcome.VALUE_LOST] ?: 0,
        counts[Outcome.ESCAPED] ?: 0,
    )
}

/**
 * Runs [scenario] [RunParams.runs] times.
 *
 * Call it on the main dispatcher: Dispatchers.Main.immediate on Android (viewModelScope),
 * or a single-thread dispatcher in JVM tests.
 */
suspend fun runScenario(
    scenario: Scenario,
    repo: ProfileRepository,
    params: RunParams,
    onProgress: (done: Int) -> Unit = {},
): ScenarioResult {
    val counts = linkedMapOf<Outcome, Int>()
    for (i in 1..params.runs) {
        var outcome = Outcome.OK
        coroutineScope {
            // Start collecting right away, like viewModelScope (Dispatchers.Main.immediate) does.
            val job = launch(start = CoroutineStart.UNDISPATCHED) {
                outcome = scenario.runOnce(repo, params.failAfterMs)
            }
            // Keep the main thread busy for a moment, as it is while a screen draws its first frame.
            if (params.mainBusyMs > 0) Thread.sleep(params.mainBusyMs)
            job.join()
        }
        counts[outcome] = (counts[outcome] ?: 0) + 1
        if (i % 10 == 0) {
            onProgress(i)
            yield() // let the UI draw progress between batches
        }
    }
    return ScenarioResult(scenario, params, counts)
}
