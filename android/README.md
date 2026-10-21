# CatchTrapAndroid

A small Android app that runs kotlinx.coroutines issue #4159 ("Flow.catch skips upstream exceptions")
with real parts only:

- a real `ViewModel` on the real `viewModelScope` (`Dispatchers.Main.immediate`, no exception handler)
- a Room database file with one cached profile
- Retrofit + OkHttp making a real request
- a real crash: the process dies and Logcat prints `FATAL EXCEPTION`

Nothing is delayed artificially. Whether a scenario crashes depends on the device's real timing.

## The code under test

`ProfileRepository.profile()` is the shape from the official Android guide:

```kotlin
flow {
    dao.cachedProfile()?.let { emit(it) }   // Room, blocking read
    emit(fetch())                           // Retrofit
}.flowOn(io)
```

`ProfileViewModel` adds `catch` after it, in three ways: `loadOnce()` (`first()`), `observe()`
(a collector that never suspends) and `observeAndPersist()` (a collector that calls a suspending Room
write). Each has a `...Fixed()` twin that uses `profileResult()`, where `catch` sits before `flowOn`.

## Run the app

Open the folder in Android Studio and press Run, or:

```bash
./gradlew installDebug
adb shell am start -n me.elwan.catchtrap/.ui.MainActivity
adb logcat -s AndroidRuntime      # the crash, when it happens
adb logcat -s CatchTrap:E         # the full stack trace of that crash
```

**Why two tags.** Android prints no stack trace for a crash whose exception is, or is caused by, an
`UnknownHostException`: `android.util.Log.printlns` drops it on purpose. Under `AndroidRuntime` you
see only `FATAL EXCEPTION: main` and the process line. `CatchTrapApp` therefore logs the trace as
plain text under `CatchTrap` and then passes the exception on, so the app still crashes for real.

On screen:

1. **Network host.** "Unresolvable host" (`https://profile-api.invalid/`) always fails with a real
   `UnknownHostException`. "Real host" (`https://api.github.com/`) works online; turn on airplane
   mode to make it fail.
2. **Network call.** A Retrofit `suspend fun`, or a blocking `Call.execute()`.
3. **Scenario buttons.** The top three have `catch` after `flowOn`. If the error skips `catch`, the
   app crashes. The bottom three are the fix.
4. **Cold start.** Tick the box, kill the app, launch it again: `observe()` starts before the first
   frame. Look at "States given to the screen" to see whether the cached profile ever arrived.
5. **Measure.** Runs every scenario 40 times and counts. While it runs, the default
   uncaught-exception handler is replaced so the process survives; nothing else is changed.

## Run the on-device tests

With a device or emulator connected:

```bash
./gradlew connectedDebugAndroidTest
adb logcat -s CatchTrap           # the counts per scenario
```

To run every test class in rounds and keep the counts, use `./run-device-tests.sh` (see the main
README). With more than one device connected, choose one by putting
`ANDROID_SERIAL=<serial from adb devices>` in front of either command.

`CatchTrapOnDeviceTest` runs the profile scenarios above:

| Test | What it does |
|---|---|
| `loadOnce_withBlockingCall_errorSkipsCatch` | Asserts the crash happens at least once in 40 runs. Should start failing when #4159 is fixed. |
| `fixedRepository_neverCrashes_andAlwaysShowsTheCachedProfile` | Asserts 0 crashes and the cached profile in every run. |
| `report_vulnerableScenarios` | Counts every vulnerable scenario with both call styles. Asserts nothing. |

`LifecycleAwareOnDeviceTest` opens a real activity, so keep the screen on. `report_lifecycleAware`
checks whether lifecycle-aware collection changes the trap, with `catch` after `flowOn` in every row:
`collectAsStateWithLifecycle()` and `repeatOnLifecycle(STARTED)`, and for comparison `collectAsState()`
and a snackbar shown from a `LaunchedEffect`. The call fails while the screen is visible, or the app
goes to the background (a real `onStop`) while the call is in progress and then comes back. Reaching
`onStop` takes about as long as the call, so in the background-trip rows the call nearly always fails
just before the app is in the background: they show that the trip there and back doesn't change the
result, not a failure in the background. `report_stateInViewModel` runs the article's profile ViewModel,
`stateIn(WhileSubscribed(5_000))` read with `collectAsStateWithLifecycle()`, on screen, during a
background trip and when the user leaves, then the same with `catch` before `flowOn`. Its `callMs = 0`
rows are a cold start offline: does the screen draw the cached profile before the error state replaces
it?

`MainSafeCallsTest` checks the advice to delete `flowOn` when every call in the builder is main-safe.
Each call is made from the main thread with Room's and Retrofit's default setup, and the test records
the thread the SQL and the HTTP request actually run on. Without `flowOn`, nothing ran on the main
thread and the ViewModel's `catch` worked; `withFlowOn_control` runs the same chain with `flowOn`, to
show it is one the trap hits.

`FlowOnMainImmediateTest` checks that `.flowOn(Dispatchers.Main)` inside `viewModelScope` creates a
channel even though everything stays on the main thread: `viewModelScope` runs on
`Dispatchers.Main.immediate`, which counts as a different dispatcher. `flowOn(Dispatchers.Main.immediate)`
and no `flowOn` are the controls.

`NotificationsOnDeviceTest` runs the article's lead example, the chat app's notifications as Wire shows
them: a real Room Flow of the messages with no notification yet, and an observer that runs once in an
app-wide `CoroutineScope(SupervisorJob() + Dispatchers.Default)` with no exception handler. For each list
it posts the notifications (a stand-in that then waits 100 ms, as Wire's `handleNotification` does) and
marks the messages as notified in Room. A poll arrives either while the observer waits after posting or
after it finished. `report_notifications` counts crashes with `catch` after `flowOn`, how many of those
crashes have the article's trace fingerprint (a suppressed cancellation caused by the crash itself, printed
as `CIRCULAR REFERENCE`), and what happens after the poll with the per-message fix.
`catchBeforeFlowOn_neverCrashes` asserts the fix. `report_onEach` moves the work into `onEach` before
`catch`, with and without `flowOn` after it. `report_collectLatest` counts crashes, and whether the first
message is posted twice when a second one arrives during the wait, with plain `collect` as the comparison.
Each run waits for Room's first query before the first message arrives, so that query can't race the
insert and emit the message twice.

```bash
CLASS=me.elwan.catchtrap.NotificationsOnDeviceTest ./run-device-tests.sh 1 20
```

`OrderTrackingOnDeviceTest` needs no network. A real Room Flow observes an order. The test writes a
status this app version does not know (handled), then ON_THE_WAY with no driver, as for an order handed
to a partner courier, and the repository's mapper throws because ON_THE_WAY requires a driver. The
ViewModel's `catch` sits after the repository's `flowOn` and sends one snackbar message per update.
`report_orderTracking` counts crashes per message style; `catchBeforeFlowOn_neverCrashes` asserts the
fix and that the mapper really threw. `report_pileUp` sends many updates at once through the same
repository chain, then the one without a driver: 63 and 66 updates for `Channel(BUFFERED)` and `shareIn`,
which hold 64 messages, and 100 for the `StateFlow` styles. Room merges quick writes, so it can't make the
pile-up itself. The update without a driver is sent only once the ViewModel has received all the others (or after one
second), so an update still in the repository's own `flowOn` queue can't decide the row. Each row prints how many
updates the ViewModel and the UI received.

The network tests use the unresolvable host, so they behave the same online and offline. Timing does differ:
offline, the DNS failure is almost immediate; online, it takes a network round trip.

## Versions

`gradle/libs.versions.toml`: AGP 9.1.0 (built-in Kotlin), Kotlin 2.4.10, KSP 2.3.6, Room 2.8.4,
Retrofit 3.0.0, kotlinx.coroutines 1.11.0, Gradle 9.4.1. Change `kotlinx-coroutines` to compare releases.
