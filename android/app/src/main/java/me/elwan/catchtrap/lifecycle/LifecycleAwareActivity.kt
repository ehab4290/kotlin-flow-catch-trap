package me.elwan.catchtrap.lifecycle

import android.content.Context
import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ViewModel
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.repeatOnLifecycle
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.initializer
import androidx.lifecycle.viewmodel.viewModelFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.stateIn
import me.elwan.catchtrap.data.ProfileApi
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Lifecycle-aware collection with real parts: Compose, collectAsStateWithLifecycle, repeatOnLifecycle,
 * Retrofit, and the activity's real lifecycle (the test sends it to the background and back).
 *
 * The flow is the profile screen's: the cached value, then a network call that fails after CALL_MS.
 * flowOn is the repository's, catch is the ViewModel's, so catch sits after the channel.
 */
enum class Collector(val label: String) {
    STATE_WITH_LIFECYCLE("collectAsStateWithLifecycle()"),
    REPEAT_STATE("repeatOnLifecycle(STARTED) { collect { state = it } }"),
    REPEAT_SNACKBAR("repeatOnLifecycle(STARTED) { collect { showSnackbar(it) } }"),
    COLLECT_AS_STATE("collectAsState(), not lifecycle-aware"),
    LAUNCHED_EFFECT_SNACKBAR("LaunchedEffect { collect { showSnackbar(it) } }, not lifecycle-aware"),
    /** The article's profile ViewModel: stateIn(WhileSubscribed(5_000)), read with collectAsStateWithLifecycle(). */
    STATE_IN("stateIn(WhileSubscribed(5_000)), catch after flowOn"),
    STATE_IN_SAFER("stateIn(WhileSubscribed(5_000)), safer way: catch before flowOn"),
}

enum class NetworkCall { SUSPENDING, BLOCKING }

class LifecycleProfileViewModel(api: ProfileApi, call: NetworkCall) : ViewModel() {
    /** The test run this ViewModel belongs to. A callback that lands after the next run has started counts as late. */
    private val run = LifecycleAwareActivity.runId

    private val cachedThenFresh: Flow<String> = flow {
        emit(LifecycleAwareActivity.CACHED)
        LifecycleAwareActivity.count(run, LifecycleAwareActivity.callsStarted)
        val body = when (call) {
            NetworkCall.SUSPENDING -> api.fetchProfile()
            NetworkCall.BLOCKING -> api.fetchProfileBlocking().execute().body() ?: throw IOException("no body")
        }
        emit(body.use { it.string() })
    }

    private fun fallback(): String {
        LifecycleAwareActivity.count(run, LifecycleAwareActivity.catchRan)
        return LifecycleAwareActivity.FALLBACK
    }

    val profile: Flow<String> = cachedThenFresh
        .flowOn(Dispatchers.IO)        // the repository
        .catch { emit(fallback()) }    // the ViewModel

    /** As in the article: map, catch, then stateIn with a 5 s stop timeout. */
    val uiState: StateFlow<String> = profile
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), LifecycleAwareActivity.LOADING)

    /** The safer way: catch before flowOn, nothing to catch in the ViewModel. */
    val uiStateSafer: StateFlow<String> = cachedThenFresh
        .catch { emit(fallback()) }
        .flowOn(Dispatchers.IO)
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), LifecycleAwareActivity.LOADING)

    override fun onCleared() {
        LifecycleAwareActivity.viewModelCleared = true
    }
}

class LifecycleAwareActivity : ComponentActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val collector = Collector.valueOf(intent.getStringExtra(EXTRA_COLLECTOR)!!)
        val call = NetworkCall.valueOf(intent.getStringExtra(EXTRA_CALL)!!)
        callMs = intent.getLongExtra(EXTRA_CALL_MS, CALL_MS)
        lifecycle.addObserver(LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_STOP -> inBackground = true
                Lifecycle.Event.ON_START -> inBackground = false
                else -> Unit
            }
        })
        val viewModel: LifecycleProfileViewModel by viewModels {
            viewModelFactory { initializer { LifecycleProfileViewModel(slowFailingApi(), call) } }
        }
        setContent { MaterialTheme { ProfileScreen(viewModel, collector) } }
    }

    /** Retrofit with an interceptor that takes CALL_MS and then fails, as a flaky network would. */
    private fun slowFailingApi(): ProfileApi {
        val run = runId
        val client = OkHttpClient.Builder()
            .addInterceptor {
                if (callMs > 0) Thread.sleep(callMs)
                if (inBackground) count(run, callsEndedInBackground)
                throw IOException("network down")
            }
            .build()
        return Retrofit.Builder().baseUrl("http://example.invalid/").client(client).build().create(ProfileApi::class.java)
    }

    companion object {
        const val EXTRA_COLLECTOR = "collector"
        const val EXTRA_CALL = "call"
        const val CACHED = "Octocat (cached)"
        const val FALLBACK = "fallback"
        const val LOADING = "loading"
        const val EXTRA_CALL_MS = "callMs"
        const val CALL_MS = 1_000L

        /** How long this activity's network call takes before it fails. */
        @Volatile var callMs = CALL_MS

        /** Every value the screen showed (state collectors) or started to show in a snackbar. */
        val shown = CopyOnWriteArrayList<String>()
        val catchRan = AtomicInteger()
        val callsStarted = AtomicInteger()
        val callsEndedInBackground = AtomicInteger()
        @Volatile var inBackground = false
        @Volatile var viewModelCleared = false

        /** Which test run is in progress. Each run's ViewModel and network client capture it when created. */
        @Volatile var runId = 0
        /** Callbacks from an earlier run (a blocking call that ended after the next run started). Not counted above. */
        val late = AtomicInteger()

        /** Counts [counter] only if [run] is still the current run; otherwise counts it as late. */
        fun count(run: Int, counter: AtomicInteger) {
            if (run == runId) counter.incrementAndGet() else late.incrementAndGet()
        }

        fun reset() {
            runId++
            shown.clear(); catchRan.set(0); callsStarted.set(0); callsEndedInBackground.set(0); late.set(0)
            inBackground = false; viewModelCleared = false
        }

        fun intent(context: Context, collector: Collector, call: NetworkCall, callMs: Long = CALL_MS): Intent =
            Intent(context, LifecycleAwareActivity::class.java)
                .putExtra(EXTRA_COLLECTOR, collector.name)
                .putExtra(EXTRA_CALL, call.name)
                .putExtra(EXTRA_CALL_MS, callMs)
    }
}

@Composable
private fun ProfileScreen(viewModel: LifecycleProfileViewModel, collector: Collector) {
    val snackbarHostState = remember { SnackbarHostState() }
    val lifecycleOwner = LocalLifecycleOwner.current
    val shown = LifecycleAwareActivity.shown

    val value: String? = when (collector) {
        Collector.STATE_WITH_LIFECYCLE ->
            viewModel.profile.collectAsStateWithLifecycle(LifecycleAwareActivity.LOADING).value
        Collector.COLLECT_AS_STATE ->
            viewModel.profile.collectAsState(LifecycleAwareActivity.LOADING).value
        Collector.STATE_IN -> viewModel.uiState.collectAsStateWithLifecycle().value
        Collector.STATE_IN_SAFER -> viewModel.uiStateSafer.collectAsStateWithLifecycle().value
        Collector.REPEAT_STATE -> {
            var state by remember { mutableStateOf(LifecycleAwareActivity.LOADING) }
            LaunchedEffect(Unit) {
                lifecycleOwner.repeatOnLifecycle(Lifecycle.State.STARTED) {
                    viewModel.profile.collect { state = it }
                }
            }
            state
        }
        Collector.REPEAT_SNACKBAR -> {
            LaunchedEffect(Unit) {
                lifecycleOwner.repeatOnLifecycle(Lifecycle.State.STARTED) {
                    viewModel.profile.collect { shown += it; snackbarHostState.showSnackbar(it) }
                }
            }
            null
        }
        Collector.LAUNCHED_EFFECT_SNACKBAR -> {
            LaunchedEffect(Unit) {
                viewModel.profile.collect { shown += it; snackbarHostState.showSnackbar(it) }
            }
            null
        }
    }
    if (value != null && value != LifecycleAwareActivity.LOADING) {
        LaunchedEffect(value) { shown += value }
    }

    Scaffold(snackbarHost = { SnackbarHost(snackbarHostState) }) { padding ->
        Column(Modifier.fillMaxSize().padding(padding).padding(16.dp)) {
            Text(collector.label, style = MaterialTheme.typography.titleMedium)
            if (value != null) Text("Profile: $value")
        }
    }
}
