package catchtrap

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import catchtrap.androidlike.runAndroidLikeReport
import catchtrap.checks.runExtraChecks
import catchtrap.fixes.runCatchAwareChecks
import catchtrap.fixes.runFixOptions
import catchtrap.flowcatch.ProfileRepository
import catchtrap.flowcatch.coroutinesVersion
import catchtrap.flowcatch.newFakeMainDispatcher
import catchtrap.flowcatch.runGrid
import catchtrap.operators.runOperatorMatrix
import catchtrap.trace.runTraces

/**
 * Prints one of the reports. The optional number is the runs per row (default 200).
 *
 *   ./gradlew run --args="android"         a ViewModel-shaped app: does the error crash it?
 *   ./gradlew run --args="operators"       which operators before catch break it?
 *   ./gradlew run --args="grid"            flowOn + catch under different timings
 *   ./gradlew run --args="checks"          early-stop operators, retry, try/catch, shareIn
 *   ./gradlew run --args="trace"           catchImpl's decisions, line by line
 *   ./gradlew run --args="fixes"           the ways out of the trap, side by side; what catchAware costs
 *   ./gradlew run --args="android 500"     any report, 500 runs per row
 */
fun main(args: Array<String>) {
    val report = args.firstOrNull { it.toIntOrNull() == null } ?: "android"
    val runs = args.firstNotNullOfOrNull { it.toIntOrNull() } ?: 200
    println("kotlinx.coroutines ${coroutinesVersion()}, $runs runs per row")

    when (report) {
        "android" -> {
            println("crashed = an exception reached the default uncaught-exception handler (FATAL EXCEPTION on Android)")
            runAndroidLikeReport(runs)
        }
        "operators" -> {
            println("ok = \"cached\" delivered, lost = \"cached\" dropped, escaped = the error was thrown past catch")
            runOperatorMatrix(runs)
        }
        "grid" -> {
            println("ok = cached value delivered, lost = cached value dropped, escaped = error thrown past catch/retry")
            newFakeMainDispatcher().use { main ->
                runBlocking(main) {
                    runGrid(ProfileRepository(Dispatchers.IO), runs)
                }
            }
        }
        "checks" -> {
            println("everything is collected on a single main thread")
            runExtraChecks(runs)
        }
        "trace" -> runTraces()
        "fixes" -> {
            println("ok = \"cached\" delivered, lost = \"cached\" dropped, escaped = the error was thrown to the caller")
            runFixOptions(runs)
            runCatchAwareChecks(runs)
        }
        else -> println("Unknown report \"$report\". Use: android, operators, grid, checks, trace or fixes.")
    }
}
