package catchtrap.standalone.statein

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import catchtrap.androidlike.FakeAndroid
import java.net.UnknownHostException
import java.util.concurrent.CopyOnWriteArrayList

/*
 * The profile screen with the ViewModel the article shows:
 *   repository.profile().map { Content(it) }.catch { emit(Error(..)) }
 *       .stateIn(viewModelScope, SharingStarted.WhileSubscribed(stopMs), Loading)
 * and the safer way: repository.profileResult().map { it.toUiState() }.stateIn(...)
 * The screen collects uiState on the main thread, as collectAsStateWithLifecycle() does.
 * For comparison, the old ViewModel: viewModelScope.launch { ....collect { _uiState.value = it } }.
 *
 * Counted per run:
 *   crashed        an exception reached the default uncaught-exception handler
 *   state cached   the StateFlow held the cached profile at some point (what the old report counted)
 *   screen cached  the screen's collector received the cached profile
 *   screen error   the screen's collector received the error state
 */
sealed interface UiState {
    data object Loading : UiState
    data class Content(val name: String) : UiState
    data class Error(val message: String) : UiState
}
enum class Call(val label: String) { BLOCKING("blocking call"), SUSPENDING("suspending call"), IMMEDIATE("fails immediately") }

class Repo(private val call: Call, private val callMs: Long) {
    private fun cachedThenFresh(): Flow<String> = flow {
        emit("cached")
        when (call) { Call.BLOCKING -> Thread.sleep(callMs); Call.SUSPENDING -> delay(callMs); Call.IMMEDIATE -> Unit }
        throw UnknownHostException("Unable to resolve host \"api.example.com\"")
    }
    fun profile(): Flow<String> = cachedThenFresh().flowOn(Dispatchers.IO)
    fun profileResult(): Flow<Result<String>> =
        cachedThenFresh().map { Result.success(it) }.catch { emit(Result.failure(it)) }.flowOn(Dispatchers.IO)
}

enum class Vm(val label: String) {
    STATE_IN("stateIn, catch after flowOn"),
    STATE_IN_SAFER("stateIn, safer way (catch before flowOn)"),
    LAUNCH_COLLECT("old: launch { collect { _uiState.value = it } }, catch after flowOn"),
}

class ViewModel(vm: Vm, repo: Repo, val scope: CoroutineScope, stopMs: Long) {
    val reached = CopyOnWriteArrayList<UiState>()
    val uiState: StateFlow<UiState> = when (vm) {
        Vm.STATE_IN -> repo.profile()
            .map<String, UiState> { UiState.Content(it) }
            .catch { emit(UiState.Error(it.message ?: "Unknown error")) }
            .onEach { reached += it }
            .stateIn(scope, SharingStarted.WhileSubscribed(stopMs), UiState.Loading)
        Vm.STATE_IN_SAFER -> repo.profileResult()
            .map { r -> r.getOrNull()?.let<String, UiState> { UiState.Content(it) } ?: UiState.Error(r.exceptionOrNull()?.message ?: "Unknown error") }
            .onEach { reached += it }
            .stateIn(scope, SharingStarted.WhileSubscribed(stopMs), UiState.Loading)
        Vm.LAUNCH_COLLECT -> {
            val s = MutableStateFlow<UiState>(UiState.Loading)
            scope.launch {
                repo.profile()
                    .map<String, UiState> { UiState.Content(it) }
                    .catch { emit(UiState.Error(it.message ?: "Unknown error")) }
                    .collect { reached += it; s.value = it }
            }
            s
        }
    }
}

enum class Scene(val label: String) {
    IDLE("main idle"),
    FIRST_FRAME("main busy 16 ms before the screen subscribes"),
    FRAME_AFTER_SUBSCRIBE("main busy 16 ms right after the screen subscribes"),
    CLEARED("user leaves, viewModelScope cleared during the call"),
    STOP_TIMEOUT("user leaves, WhileSubscribed timeout fires during the call"),
}

/** Compose's frame dispatcher: same main thread, but every resume is posted, never run in place. */
class PostingMain(private val main: CoroutineDispatcher) : CoroutineDispatcher() {
    override fun isDispatchNeeded(context: kotlin.coroutines.CoroutineContext) = true
    override fun dispatch(context: kotlin.coroutines.CoroutineContext, block: Runnable) = main.dispatch(context, block)
}

fun main() {
    val runs = System.getenv("RUNS")?.toInt() ?: 200
    println("kotlinx.coroutines 1.11.0, $runs runs per row; the screen collects on a posting main dispatcher, like Compose")
    FakeAndroid().use { android ->
        for (scene in Scene.entries.filter { System.getenv("SCENES")?.split(",")?.contains(it.name) ?: true }) {
            println()
            for (call in Call.entries) for (vm in Vm.entries) {
                if (scene == Scene.STOP_TIMEOUT && vm == Vm.LAUNCH_COLLECT) continue // no WhileSubscribed there
                // Leaving needs a call that is still running when the screen goes: 30 ms calls,
                // a 10 ms stop timeout standing in for 5 s. Otherwise a 5 ms call, as in the android report.
                val callMs = if (scene == Scene.CLEARED || scene == Scene.STOP_TIMEOUT) 30L else 5L
                val stopMs = if (scene == Scene.STOP_TIMEOUT) 10L else 5_000L
                var crashed = 0; var stateCached = 0; var screenCached = 0; var screenError = 0
                repeat(runs) { runIdx ->
                    if (System.getenv("DEBUG") != null) System.err.println("run $runIdx start")
                    android.crashes.clear()
                    val vmScope = android.newViewModelScope()
                    val model = ViewModel(vm, Repo(call, callMs), vmScope, stopMs)
                    val screenSaw = CopyOnWriteArrayList<UiState>()
                    android.onMainThread {
                        val screen = CoroutineScope(SupervisorJob() + PostingMain(android.main))
                        screen.launch { model.uiState.collect { screenSaw += it } }  // collectAsStateWithLifecycle()
                        when (scene) {
                            Scene.IDLE -> Unit
                            Scene.FIRST_FRAME -> Thread.sleep(16)
                            Scene.FRAME_AFTER_SUBSCRIBE -> screen.launch { Thread.sleep(16) } // posted after the collector starts: layout and draw
                            Scene.CLEARED -> { delay(5); screen.cancel(); vmScope.cancel() }   // back pressed: onCleared()
                            Scene.STOP_TIMEOUT -> { delay(5); screen.cancel() }                 // app to background
                        }
                        if (scene == Scene.CLEARED || scene == Scene.STOP_TIMEOUT) delay(callMs + 40)
                        else {
                            val deadline = System.currentTimeMillis() + 300
                            while (System.currentTimeMillis() < deadline && android.crashes.isEmpty() &&
                                screenSaw.none { it is UiState.Error }) delay(1)
                            delay(callMs + 20) // let a late crash land
                        }
                        if (System.getenv("DEBUG") != null) System.err.println("run $runIdx body done")
                        screen.cancel(); vmScope.cancel()
                    }
                    if (android.crashes.isNotEmpty()) crashed++
                    if (model.reached.any { it == UiState.Content("cached") }) stateCached++
                    if (screenSaw.any { it == UiState.Content("cached") }) screenCached++
                    if (screenSaw.any { it is UiState.Error }) screenError++
                }
                println(String.format("%-56s %-18s %-68s | crashed %3d  state cached %3d  screen cached %3d  screen error %3d  / %d",
                    scene.label, call.label, vm.label, crashed, stateCached, screenCached, screenError, runs))
            }
        }
    }
}
