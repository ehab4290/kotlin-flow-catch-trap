package catchtrap.androidlike

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay

/** What happens on the screen during one run. */
enum class UserAction(val title: String) {
    LOAD_ONCE("loadOnce()"),
    OBSERVE_FIRST_FRAME("observe(), main busy 16 ms (first frame)"),
    OBSERVE_AND_PERSIST("observeAndPersist()"),
    OBSERVE_THEN_LEAVE("observe(), user leaves after 2 ms"),
    OBSERVE_PERSIST_THEN_LEAVE("observeAndPersist(), user leaves after 2 ms"),
}

data class AndroidLikeRow(
    val action: UserAction,
    val style: NetworkStyle,
    val fixed: Boolean,
    val runs: Int,
    /** Runs in which an exception reached the default uncaught-exception handler. */
    val crashes: Int,
    /** Runs in which the screen was given the cached profile. */
    val cachedShown: Int,
    val sampleCrash: Throwable?,
) {
    fun summary(): String = String.format(
        "%-44s %-34s %-22s | crashed %3d / %d   cached profile shown %3d / %d",
        action.title,
        style.description,
        if (fixed) "catch before flowOn" else "catch after flowOn",
        crashes, runs, cachedShown, runs,
    )
}

fun runAndroidLikeRow(
    android: FakeAndroid,
    action: UserAction,
    style: NetworkStyle,
    fixed: Boolean,
    runs: Int,
): AndroidLikeRow {
    var crashes = 0
    var cachedShown = 0
    var sample: Throwable? = null

    repeat(runs) {
        android.crashes.clear()
        val dao = ProfileDao()
        val repository = ProfileRepository(dao, ProfileApi(style), Dispatchers.IO)
        val viewModel = ProfileViewModel(repository, dao, android.newViewModelScope())

        android.onMainThread {
            when (action) {
                UserAction.LOAD_ONCE -> {
                    val job = if (fixed) viewModel.loadOnceFixed() else viewModel.loadOnce()
                    job.join()
                }
                UserAction.OBSERVE_FIRST_FRAME -> {
                    val job = if (fixed) viewModel.observeFixed() else viewModel.observe()
                    Thread.sleep(16) // the main thread is busy drawing the first frame
                    job.join()
                }
                UserAction.OBSERVE_AND_PERSIST -> {
                    val job = if (fixed) viewModel.observeAndPersistFixed() else viewModel.observeAndPersist()
                    job.join()
                }
                UserAction.OBSERVE_THEN_LEAVE -> {
                    val job = if (fixed) viewModel.observeFixed() else viewModel.observe()
                    delay(2)
                    viewModel.onCleared() // the user presses back
                    job.join()
                }
                UserAction.OBSERVE_PERSIST_THEN_LEAVE -> {
                    val job = if (fixed) viewModel.observeAndPersistFixed() else viewModel.observeAndPersist()
                    delay(2)
                    viewModel.onCleared()
                    job.join()
                }
            }
        }

        if (android.crashes.isNotEmpty()) {
            crashes++
            if (sample == null) sample = android.crashes.first()
        }
        val sawCached = viewModel.shown.any { it is ProfileUiState.Content && it.profile.origin == "cache" }
        if (sawCached) cachedShown++
    }
    return AndroidLikeRow(action, style, fixed, runs, crashes, cachedShown, sample)
}

/** Runs every action for every network style, vulnerable and fixed, printing each row as it finishes. */
fun runAndroidLikeReport(runs: Int, print: (String) -> Unit = ::println): List<AndroidLikeRow> {
    val rows = mutableListOf<AndroidLikeRow>()
    FakeAndroid().use { android ->
        for (action in UserAction.entries) {
            print("")
            for (style in NetworkStyle.entries) {
                for (fixed in listOf(false, true)) {
                    val row = runAndroidLikeRow(android, action, style, fixed, runs)
                    print(row.summary())
                    rows += row
                }
            }
        }
    }
    rows.firstOrNull { !it.fixed && it.action == UserAction.LOAD_ONCE && it.sampleCrash != null }
        ?.let { print(""); print(logcat("loadOnce()", it.sampleCrash!!)) }
    rows.firstOrNull { !it.fixed && it.action == UserAction.OBSERVE_AND_PERSIST && it.sampleCrash != null }
        ?.let { print(""); print(logcat("observeAndPersist()", it.sampleCrash!!)) }
    rows.firstOrNull { !it.fixed && it.action == UserAction.OBSERVE_PERSIST_THEN_LEAVE && it.sampleCrash != null }
        ?.let { print(""); print(logcat("observeAndPersist(), user leaves", it.sampleCrash!!)) }
    return rows
}

/** Formats a crash the way Logcat shows it, trimmed to the lines that matter. */
fun logcat(title: String, crash: Throwable): String = buildString {
    appendLine("---- $title: what Logcat would show")
    appendLine("FATAL EXCEPTION: main")
    appendLine(crash.toString())
    crash.stackTrace.take(6).forEach { appendLine("    at $it") }
    crash.suppressed.forEach { appendLine("    Suppressed: $it") }
}.trimEnd()
