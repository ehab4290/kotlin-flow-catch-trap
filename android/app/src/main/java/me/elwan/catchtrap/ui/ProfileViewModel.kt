package me.elwan.catchtrap.ui

import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.initializer
import androidx.lifecycle.viewmodel.viewModelFactory
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import me.elwan.catchtrap.NetworkSetup
import me.elwan.catchtrap.Profile
import me.elwan.catchtrap.ProfileUiState
import me.elwan.catchtrap.Scenario
import me.elwan.catchtrap.data.ProfileRepository

/**
 * A real `ViewModel` on the real `viewModelScope` (SupervisorJob + Dispatchers.Main.immediate,
 * no CoroutineExceptionHandler). A coroutine launched here that fails crashes the app.
 */
class ProfileViewModel(private val repository: ProfileRepository) : ViewModel() {

    private val _states = MutableStateFlow<List<ProfileUiState>>(emptyList())

    /** Every state the screen was given since the last scenario started, in order. */
    val states: StateFlow<List<ProfileUiState>> = _states.asStateFlow()

    private fun show(state: ProfileUiState) {
        _states.update { it + state }
    }

    fun run(scenario: Scenario, setup: NetworkSetup): Job {
        _states.value = emptyList()
        return when (scenario) {
            Scenario.LOAD_ONCE -> loadOnce(setup)
            Scenario.OBSERVE -> observe(setup)
            Scenario.OBSERVE_AND_PERSIST -> observeAndPersist(setup)
            Scenario.LOAD_ONCE_FIXED -> loadOnceFixed(setup)
            Scenario.OBSERVE_FIXED -> observeFixed(setup)
            Scenario.OBSERVE_AND_PERSIST_FIXED -> observeAndPersistFixed(setup)
        }
    }

    // ---- catch AFTER flowOn ---------------------------------------------------------------------

    /** "Give me the profile once, and a placeholder if anything goes wrong." */
    fun loadOnce(setup: NetworkSetup): Job = viewModelScope.launch {
        val profile = repository.profile(setup)
            .catch { emit(Profile.PLACEHOLDER) }
            .first()
        show(ProfileUiState.Content(profile))
    }

    /** "Show every update, and an error state if the stream fails." */
    fun observe(setup: NetworkSetup): Job = viewModelScope.launch {
        repository.profile(setup)
            .map<Profile, ProfileUiState> { ProfileUiState.Content(it) }
            .catch { emit(ProfileUiState.Error(it.describe())) }
            .collect { show(it) }
    }

    /** The same, with a suspending Room write for every profile that arrives. */
    fun observeAndPersist(setup: NetworkSetup): Job = viewModelScope.launch {
        repository.profile(setup)
            .map<Profile, ProfileUiState> { ProfileUiState.Content(it) }
            .catch { emit(ProfileUiState.Error(it.describe())) }
            .collect { state ->
                show(state)
                if (state is ProfileUiState.Content) repository.save(state.profile)
            }
    }

    // ---- catch BEFORE flowOn (profileResult) ----------------------------------------------------

    fun loadOnceFixed(setup: NetworkSetup): Job = viewModelScope.launch {
        val profile = repository.profileResult(setup).first().getOrDefault(Profile.PLACEHOLDER)
        show(ProfileUiState.Content(profile))
    }

    fun observeFixed(setup: NetworkSetup): Job = viewModelScope.launch {
        repository.profileResult(setup).collect { show(it.toUiState()) }
    }

    fun observeAndPersistFixed(setup: NetworkSetup): Job = viewModelScope.launch {
        repository.profileResult(setup).collect { result ->
            show(result.toUiState())
            result.getOrNull()?.let { repository.save(it) }
        }
    }

    private fun Result<Profile>.toUiState(): ProfileUiState = fold(
        onSuccess = { ProfileUiState.Content(it) },
        onFailure = { ProfileUiState.Error(it.describe()) },
    )

    private fun Throwable.describe(): String = "${javaClass.simpleName}: $message"

    companion object {
        fun factory(repository: ProfileRepository): ViewModelProvider.Factory = viewModelFactory {
            initializer { ProfileViewModel(repository) }
        }
    }
}
