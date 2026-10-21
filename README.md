# kotlin-flow-catch-trap

The code behind the article
**[The catch That Didn’t: Kotlin Flow’s Channel Trap](https://medium.com/p/da1bbdea80f5)**.

`catch` is only reliable while the source and the collector run in one coroutine. Put a channel
between them (`flowOn` with another dispatcher, `buffer`, `flatMapLatest`, `combine` and others) and
`catch` can drop the last value, or let the exception skip it, which crashes the app on Android.
The background is [kotlinx.coroutines issue #4159](https://github.com/Kotlin/kotlinx.coroutines/issues/4159).

Everything here runs against kotlinx.coroutines 1.11.0.

## See it in one file

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

// Repository: emits the messages to notify, then fails on a message type this version doesn't know.
fun messagesToNotify(): Flow<List<String>> = flow {
    emit(listOf("Are you coming?"))
    delay(30) // a new message arrives while the notification waits
    throw IllegalStateException("Unknown message type: POLL")
}.flowOn(Dispatchers.IO)

fun main() = runBlocking {
    // The app's scope, as in Wire. With no handler, an uncaught exception here crashes the app;
    // this handler only prints it.
    val appScope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e ->
        print("CRASH: ")
        e.printStackTrace(System.out)
    })
    appScope.launch {
        messagesToNotify()
            .catch { println("Logged: ${it.message}") }
            .collect { messages ->
                println("Notifying about ${messages.size} message(s)")
                delay(100) // notifications.show(messages): posts, then waits, as Wire's does
            }
    }.join()
}
```

The `catch` is there to log the error. It never runs. The program prints:

```text
Notifying about 1 message(s)
CRASH: java.lang.IllegalStateException: Unknown message type: POLL
	at ...
	Suppressed: kotlinx.coroutines.JobCancellationException: ScopeCoroutine is cancelling; ...
	Caused by: [CIRCULAR REFERENCE: java.lang.IllegalStateException: Unknown message type: POLL]
```

The collector is suspended in `delay` (the wait after posting) when the source fails. The failure cancels
the collector, `catch` records that cancellation as a failure of its own downstream, and so it rethrows
the source's exception instead of calling your block. Move `.catch { … }` above
`.flowOn(Dispatchers.IO)` and the program prints `Logged: Unknown message type: POLL` instead. That
version is `CatchTrapSnippetFix.kt`.

To run both:

```bash
cd jvm
./gradlew -q installDist
java -cp 'build/install/jvm-checks/lib/*' catchtrap.standalone.snippet.CatchTrapSnippetKt
java -cp 'build/install/jvm-checks/lib/*' catchtrap.standalone.snippetfix.CatchTrapSnippetFixKt
```

`snippet/` has the same two files without a `package` line, to paste into a scratch file in any
project that has kotlinx.coroutines.

## What's in the repo

| Folder | What it is |
|---|---|
| `snippet/` | The one-file reproduction above, and its fixed version. |
| `jvm/` | Plain Kotlin/JVM checks for the mechanics: which operators create a channel, when the error skips `catch`, when the value is lost, and how each way out holds up. No Android needed. |
| `android/` | A small Android app with Room, Retrofit and Compose, and on-device tests for everything Android-specific. See [android/README.md](android/README.md). |

## Run the JVM checks

You need JDK 17 or later. The Gradle wrapper (Gradle 9.4.1) downloads the rest.

One report at a time:

```bash
cd jvm
./gradlew run --args="android"          # 200 runs per row
./gradlew run --args="operators 500"    # any report, 500 runs per row
./gradlew test                          # the unit tests
```

The reports in `Main.kt` are `android`, `operators`, `grid`, `checks`, `trace` and `fixes`.
The other checks are small standalone programs under `catchtrap.standalone`. The suite script runs
any of them, several rounds in a row:

```bash
./run-jvm-suite.sh                      # every check behind the article: 5 rounds × 500 runs per row, 4 at a time
./run-jvm-suite.sh 1 200 1 statein      # only the checks you name: 1 round, 200 runs, 1 at a time
./run-jvm-suite.sh 1 10000 1 retry
python3 aggregate.py results/<folder> > results/<folder>/AGGREGATED.txt   # median, lowest, highest per number
```

The full default run takes hours; `operators` alone takes over half an hour per round on a 16-core machine.
On a machine with only a few cores, run one check at a time (third number `1`): the checks measure races
between threads, and a check that waits for a free core changes who wins the race.

To check that everything works, a short run is enough: `./run-jvm-suite.sh 1 50 4 android checks`.

Each run gets its own folder, `jvm/results/<date>_<time>_<rounds>x<runs>/`, which git ignores:

- `<check>/round-N.txt`: the output of one round of one check
- `progress.txt`: when the run started, and when each check started and finished a round (`tail -f` it to watch a run)
- `machine.txt`: Java version, OS and number of cores
- `AGGREGATED.txt`: written by `aggregate.py`, if you redirect its output there
- `once/`: after a full default run, the checks that don't depend on timing, run once

## Run the Android tests

You need Android Studio (the project uses AGP 9.1.0) and one emulator or phone connected (`adb devices`
lists it). Open `android/` in Android Studio once, so it writes `local.properties` with your SDK path.
Keep the screen on (Developer options → Stay awake), because some tests open an activity. With more
than one device connected, choose one by putting `ANDROID_SERIAL=<serial from adb devices>` in front
of the command.

```bash
cd android
./run-device-tests.sh 3 100                                                      # every test: 3 rounds × 100 runs per row
CLASS=me.elwan.catchtrap.LifecycleAwareOnDeviceTest ./run-device-tests.sh 1 30   # one test class
```

A full run of every test, 3 rounds × 100 runs per row, takes a long time. To check that everything
works, `./run-device-tests.sh 1 10` is enough.

Each run gets its own folder, `android/device-results/<date>_<time>_<rounds>x<runs>/` (with the class
name at the end when you set `CLASS`), which git ignores:

- `device.txt`: the device model and Android version
- `round-N.txt`: the counts, one `CatchTrap` line per row, written while the round runs
- `round-N-tests.txt`: which tests passed or failed, complete once the round ends

To watch a run: `tail -f device-results/<run>/round-1.txt`. Most tests only count and assert
nothing, so one surprising row doesn't stop the others.

## Which check backs which claim

The rows follow the article, section by section. JVM names are suite checks (`./run-jvm-suite.sh 1 200 1 <name>`);
`android`, `operators`, `grid`, `checks`, `trace` and `fixes` are also reports (`./gradlew run --args="<name>"`). A § number is the section inside that
report. Android names are test classes in `android/app/src/androidTest`.

| Article section | Claim | JVM | Android |
|---|---|---|---|
| A crash your catch was written to prevent | The poll makes `toDomain()` throw. With `catch` in the observer, after `flowOn`: when the observer was idle, `catch` logged the error every time; when it was still waiting in `show()` after posting, the app crashed every time, the crash carried the mapper's exception, and the observer's `catch` never ran. | `notifystream` (part 1), `snippet` | `NotificationsOnDeviceTest` (`report_notifications`) |
| | With `catch` moved into the repository, right before `flowOn`, nothing crashed and the error was logged every time, for both timings. | `notifystream` (part 1) | `NotificationsOnDeviceTest` (`catchBeforeFlowOn_neverCrashes`) |
| The quieter failure | A collector that only writes UI state never crashed. When the main thread was busy with a frame as the error arrived, the cached profile rarely reached `uiState`. | `android` (`observe()` rows), `statein` | `CatchTrapOnDeviceTest`, `LifecycleAwareOnDeviceTest` |
| | A collector with one call that waits crashed every time. | `android` (`observeAndPersist()` rows) | `CatchTrapOnDeviceTest` |
| How catch decides | The three rules of `catchImpl`, in order. | `trace`, `shapes` | |
| The notification crash, step by step | The same steps on the plain JVM, without Android: the observer's `catch` never ran. | `notifystream` (part 1), `snippet` | |
| Failure 1: the lost value | The three bullets: main thread busy for a frame, idle main thread (a race, machine-dependent), `buffer()` on one thread. | `grid`, `android` (`observe()` rows), `operators`, `checks` §3 | |
| Failure 2: the error that skips catch | Case 1, a suspended collector: the notifier and the profile crashed every time after `flowOn(IO)`, and so did `buffer()` with everything on the main thread. | `android`, `notifystream`, `placement`, `extra` (T8) | `CatchTrapOnDeviceTest`, `NotificationsOnDeviceTest` |
| | Only work after `catch` counts: a suspending save before `flowOn`, or between `flowOn` and `catch`, was handled every time. | `savepos` | |
| | Case 2, early stops: `first()` crashed the app in nearly every run with a blocking call, never with a suspending call. Right after `emit`, a race for every early stop. | `cases`, `android` (`loadOnce()` rows), `fixes` and `ways` (the `first()` rows), `checks` §1, `grid` | `CatchTrapOnDeviceTest` |
| | `take` and `takeWhile` between the source and `catch` are as safe as `map`. | `operators` | |
| | Case 3, the user leaves during a blocking call: crashed in nearly every run, the network error carrying the cancellation as suppressed (`Suppressed: JobCancellationException: Job was cancelled` in the trace the `android` report prints). | `android` (`observeAndPersist()`, user leaves, and its “what Logcat would show” trace), `latest` (case 3) | |
| | Case 4, the collector throws: the caller can get the network error instead, with the bug in `suppressed`. When the bug came first, the caller got the bug itself every time; when the source failed at about the same moment, the bug was hidden in some runs and not in others (a race). | `collectorbug`, `checks` §2 | |
| | `flowOn(IO).retry(3)` with a suspending collector threw every time, and the source was almost always started only once. | `retry`, `checks` §4, `costs` (B4) | |
| The fix: catch before the channel | After the fix, `catch` ends the notifier's flow: the observer ended, and the text after the poll got no notification. Handled per message inside `map`, the notifications kept going; the poll got none and was logged each time the list changed, with no new logs once the table was quiet. | `notifystream` (part 2) | `NotificationsOnDeviceTest` (`report_notifications`) |
| | The producer's coroutine never fails, as long as the `catch` block doesn't throw. When it lets the error through, or an operator between it and the channel throws, the work in progress was cancelled and a value in the queue was sometimes lost. | `ways` (“catch before flowOn, and the error still gets through it”) | |
| | With the fixed repository, the screen never crashed, and the cached profile always reached `uiState` before the error state when the user stayed on the screen. With the error in the same frame, the screen may draw only the error state. | `android` (“catch before flowOn” rows), `statein` | `CatchTrapOnDeviceTest`, `LifecycleAwareOnDeviceTest` (the `callMs = 0` rows) |
| | The costs: `catch` runs on `io`; with a blocking call, `first()` returns only after the call has failed and the block has run, with a suspending call the block doesn't run; `retry(3)` before `flowOn` makes all four attempts, with a `catch` after it nothing throws, without it the fourth error reaches the code that called `collect`. | `costs`, `checks` §4 | |
| Is your code affected? | A state-writing collector never let the error skip `catch`, across three kinds of network failure. A state-driven snackbar is safe. | `android` (`observe()` rows), `extra` (P3) | `LifecycleAwareOnDeviceTest` |
| | `Channel(BUFFERED)` and `shareIn` hold 64 messages before the sender waits; a `SharedFlow` with no subscriber drops them; a `StateFlow` never waits. | `extra` (P6), `handoff`, `sharein` | |
| | The handoff table, on an Android device (`shareIn` and `stateIn` with `SharingStarted.Eagerly`; “in the background” means nothing collects): while a snackbar is showing and in the background, with two updates, then with 63 and 66 updates at once (around the 64), and 100 for the `StateFlow` rows. | `handoff`, `sharein` | `OrderTrackingOnDeviceTest` (`report_orderTracking`, `report_pileUp`) |
| Which operators create a channel | The four groups of the table, the two operator lists, and `flowOn(Dispatchers.IO)`, `debounce`, `sample` and `flatMapLatest` as the only “nearly”. | `operators` | |
| | `flowOn` without a dispatcher change creates no channel. `.flowOn(Dispatchers.Main)` inside `viewModelScope` does, on the main thread. On one main thread, `buffer`, `conflate`, `mapLatest`, `merge` and `combine` let the error skip `catch` every time (the main-thread half of `operators`). | `operators` | `FlowOnMainImmediateTest` |
| | With `catch` moved before the operator, nothing rethrew. In the value-lost check, `debounce` and `sample` lost the cached value every time, `combine` and `combineTransform` in some runs on a thread pool (the `Dispatchers.Default` half of the report), and `conflate` in a few runs on a thread pool on the 2-core machine. All of them drop values by design. | `operators` (the FIX columns), `placement` | |
| shareIn and stateIn | After `shareIn` and `stateIn`, `catch` never ran: the error goes to the sharing scope. | `checks` §6 | |
| | The subscribers are never told. With `WhileSubscribed`, the error still reaches the scope after the screen leaves during a blocking call. A `catch` before `stateIn`/`shareIn` prevented the crash in every run, even after `flowOn`, and the fallback was the subscribers' only signal. | `whilesub` | |
| How to recognize it in a crash report | The notification crash's trace: the mapper's exception, `Suppressed: JobCancellationException: ScopeCoroutine is cancelling`, and `Caused by: [CIRCULAR REFERENCE: …]`. | `snippet`, `notifystream` (part 1, the trace counts), `circ`, `shapes` | `NotificationsOnDeviceTest` (the trace counts) |
| | On Android, logcat shows no trace when the crash is an `UnknownHostException`; logging `stackTraceToString()` yourself shows it. | | The app: `adb logcat -s AndroidRuntime` against `-s CatchTrap` (see “Why two tags” in `android/README.md`, and `CatchTrapApp`) |
| The other ways out | The ways-out table. The first row's rare run that didn't rethrow usually lost the value instead. | `fixes`, `ways`, `conflate`, `latest` | |
| | Before the channel: `try` around `emit` catches the collector's exceptions; `try` around the call keeps the flow going. Without `flowOn`: Room's and Retrofit's `suspend` functions are main-safe, `withContext` around the blocking call works, `emit` inside `withContext` is refused, and `flatMapLatest`, `combine` or `buffer` bring the trap back. | `extra` (T3, T4, T6 to T8) | `MainSafeCallsTest` |
| | After the channel: `conflate()` after `catch` keeps `catch` working and lets the work in progress finish. `buffer()` after `catch` only delays the problem: once its 64 slots are full, the error skips `catch`. `buffer(Channel.UNLIMITED)` never fills and keeps every value. | `latest`, `conflate`, `buffer-between`, `buffer-nowait`, `buffer-ahead`, `extra` (T9) | |
| | At the terminal call: the work moved from `collect` into `onEach` before `catch`, with and without `flowOn` after `onEach`: `catch` ran in every run and nothing crashed. The work in progress was cancelled, because it now runs on the collecting side of the repository's channel: the first text was posted but almost never marked as notified (device runs). `.flowOn(Dispatchers.IO)` after `onEach` let the work finish only because the repository also used `Dispatchers.IO`, so the repository's own `flowOn` created no channel; with the repository on another dispatcher, its channel stayed and the work was cut off every time. | `notifystream` (part 1), `oneach` (part 1) | `NotificationsOnDeviceTest` (`report_onEach`) |
| | At the terminal call: with the work in `onEach` before `catch`, a value still in the queue was lost in some runs, as with plain `collect`; with `catch` before `flowOn`, it was kept. | `oneach` (part 2) | |
| | At the terminal call: a collector that launches its work removes case 1 only. With a `Semaphore` permit taken inside the launched work, the error never skipped `catch`; with the permit taken in the collector, the error skipped `catch` in nearly every run, as with plain `collect`. | `fixes`, `ways`, `launchwork` | |
| | At the terminal call: `collectLatest { }` after `catch` keeps `catch` working in case 1 and case 3, and so do `mapLatest`, `transformLatest` and `flatMapLatest`. A newer value cancels the work in progress, and a fallback emitted by `catch` counts as one. In the notifier, a second message during the wait gets the first one posted again, with `collectLatest` and with plain `collect` alike: the newer list was read before the first was marked as notified. When the old work is slow to stop, the error skips `catch` again. | `latest`, `notifystream` (part 3) | `NotificationsOnDeviceTest` (`report_collectLatest`) |
| | At the terminal call: a `try`/`catch` around it catches the rethrown error, but the `catch` block is skipped; with `first()`, the caller got the exception in nearly every run. | `ways`, `fixes`, `checks` §5 | |
| | At the terminal call: a `CoroutineExceptionHandler` keeps the app alive, but the `catch` block almost never ran. | `extra` (T11), `fixes` | |
| Could catch itself do better? | `catchAware` fixes case 1 after `flowOn(IO)`, `buffer()` and `flatMapLatest`, and the collector's own exceptions still reach the caller. | `fixes` | |
| | It doesn't fix the rest: `first()`, `firstOrNull()`, `take(1)` and `any { }` still rethrew; the user leaving during a blocking call still crashed. | `ways` (early stops), `fixes` | |
| | With `transform { }` or a hand-written `flow { }` wrapper after it: “Flow exception transparency is violated”. The work on the cached value was cancelled before the fallback arrived. | `msg`, `fixes` | |

The “nearly every run”, “almost never” and “depended on the machine” claims come from comparing
machines: a 16-core and a 2-core machine on the JVM, and more than one Android device. A row that is
all-or-nothing on one can show a rare exception on another.

A few statements come from reading code and documents rather than from runs:

- what `catch` and `catchImpl` do (`Errors.kt`, lines 53–57 and 147–208 in 1.11.0), and that the develop
  branch has the same `catchImpl`
- that 64 is the library's default buffer (`Channel.kt` and `Share.kt` in 1.11.0)
- the maintainer's words in #4159
- Wire's notifier: the observer runs once in `CoroutineScope(SupervisorJob() + Dispatchers.Default)` with no
  handler (`WireNotificationManager.kt`), and `handleNotification` waits 100 ms after posting
  (`MessageNotificationManager.kt`)
- Firebase AI Logic's `LiveSession` sends its audio from `onEach`, before its `catch`
- the official Android guide's `catch` at the end of the chain, and its `flowOn` section
- that `first()` after `flowOn` returns only after the producer has finished: in 1.11.0, `ChannelFlow.collect`
  is `coroutineScope { collector.emitAll(produceImpl(this)) }`, and the producer is a child of that scope
  (`ChannelFlow.kt`, lines 114–119)
- that `flow` inside `Errors.kt` is the internal `unsafeFlow`, imported under that name (line 10)
- that Retrofit's `HttpException` is not an `IOException`
- that Android's `Log` prints no stack trace for a crash caused by an `UnknownHostException` (`Log.printlns`)
- that the API reference's first sentence for `catch` ("If the upstream flow completes with an exception…")
  comes from a doc-only change on master (#4693); the 1.11.0 source reads "Catches exceptions in the flow
  completion…"

## Reading the numbers

Two kinds of result show up, and they need reading differently:

- **All-or-nothing.** `500 / 500` or `0 / 500` in every round and on every machine. That is how the
  code behaves, and the article states these exactly (“every time”, “never”).
- **Races.** The number moves between rounds and between machines, sometimes a lot. It also moves
  with the number of cores and with what else the machine is doing. Read these as “sometimes”, not as
  a rate. The article calls them races and gives no percentage.

Each row runs hundreds of times, and the suite repeats each check in several rounds, so a race shows
up in the aggregated report as a spread, such as `med 412 [376-473]` (median, lowest and highest
round), rather than as one number. That is why the repo ships no results: your machine will give
its own numbers for the races, and the same all-or-nothing results as everyone else's.

## Versions

- **kotlinx.coroutines 1.11.0**, set in `jvm/gradle/libs.versions.toml` and `android/gradle/libs.versions.toml`.
  Change it there to compare releases.
- **Kotlin 2.4.10**, **Gradle 9.4.1** (the wrapper is in both projects), **Android Gradle Plugin 9.1.0**.
- **JDK 17 or later** for the JVM checks. The Android build's Gradle daemon asks for JDK 25
  (`android/gradle/gradle-daemon-jvm.properties`), and Gradle downloads it if it is missing.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). You may copy and change the code, including
in commercial apps, as long as you keep the license and the [`NOTICE`](NOTICE) file with it. Parts of
`CatchAware.kt` and `TracedCatch.kt` come from kotlinx.coroutines, also under the Apache License 2.0;
see `NOTICE`. The article itself is not covered by this license.
