package catchtrap.androidlike

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import java.net.UnknownHostException

/*
 * An ordinary feature: DAO + API + repository + ViewModel.
 * Nothing in this file knows it is part of an experiment.
 */

data class Profile(val name: String, val origin: String) {
    companion object {
        val PLACEHOLDER = Profile(name = "Guest", origin = "placeholder")
    }
}

sealed interface ProfileUiState {
    data object Loading : ProfileUiState
    data class Content(val profile: Profile) : ProfileUiState
    data class Error(val message: String) : ProfileUiState
}

class ProfileDao {
    /** A non-suspend read, like a Room `@Query` without `suspend`. */
    fun cachedProfile(): Profile = Profile(name = "John Doe", origin = "cache")

    /** A suspending write, like a Room `@Insert suspend fun`. */
    suspend fun save(profile: Profile) {
        delay(20)
    }
}

/** How the network call behaves while the device is offline. */
enum class NetworkStyle(val description: String) {
    /** OkHttp `execute()`, `HttpURLConnection`, file or SQLite I/O. Cancellation cannot interrupt it. */
    BLOCKING("blocking call, fails after 5 ms"),

    /** A Retrofit `suspend fun`. Cancellation interrupts it. */
    SUSPENDING("suspending call, fails after 5 ms"),

    /** No waiting at all, e.g. a connectivity check that throws. */
    IMMEDIATE("fails immediately"),
}

class ProfileApi(private val style: NetworkStyle) {
    suspend fun fetchProfile(): Profile {
        when (style) {
            NetworkStyle.BLOCKING -> Thread.sleep(5)
            NetworkStyle.SUSPENDING -> delay(5)
            NetworkStyle.IMMEDIATE -> Unit
        }
        throw UnknownHostException("Unable to resolve host \"api.example.com\"")
    }
}

class ProfileRepository(
    private val dao: ProfileDao,
    private val api: ProfileApi,
    private val io: CoroutineDispatcher,
) {
    private fun cachedThenFresh(): Flow<Profile> = flow {
        emit(dao.cachedProfile())
        emit(api.fetchProfile())
    }

    /**
     * The shape from the official Android guide: `flowOn` inside the repository,
     * error handling left to whoever collects.
     */
    fun profile(): Flow<Profile> = cachedThenFresh().flowOn(io)

    /** The fix: the error becomes a value before it crosses `flowOn`. */
    fun profileResult(): Flow<Result<Profile>> =
        cachedThenFresh()
            .map { Result.success(it) }
            .catch { emit(Result.failure(it)) }
            .flowOn(io)
}

class ProfileViewModel(
    private val repository: ProfileRepository,
    private val dao: ProfileDao,
    private val viewModelScope: CoroutineScope,
) {
    val uiState = MutableStateFlow<ProfileUiState>(ProfileUiState.Loading)

    /** Every state the screen was given, in order. */
    val shown = mutableListOf<ProfileUiState>()

    private fun show(state: ProfileUiState) {
        shown += state
        uiState.value = state
    }

    // ---- catch AFTER flowOn (vulnerable) ------------------------------------------------------

    /** "Give me the profile once, and a placeholder if anything goes wrong." */
    fun loadOnce(): Job = viewModelScope.launch {
        val profile = repository.profile()
            .catch { emit(Profile.PLACEHOLDER) }
            .first()
        show(ProfileUiState.Content(profile))
    }

    /** "Show every update, and an error state if the stream fails." */
    fun observe(): Job = viewModelScope.launch {
        repository.profile()
            .map<Profile, ProfileUiState> { ProfileUiState.Content(it) }
            .catch { emit(ProfileUiState.Error(it.message ?: "Something went wrong")) }
            .collect { show(it) }
    }

    /** Same, with suspending work per value: write the profile through to another table. */
    fun observeAndPersist(): Job = viewModelScope.launch {
        repository.profile()
            .map<Profile, ProfileUiState> { ProfileUiState.Content(it) }
            .catch { emit(ProfileUiState.Error(it.message ?: "Something went wrong")) }
            .collect { state ->
                show(state)
                if (state is ProfileUiState.Content) dao.save(state.profile)
            }
    }

    // ---- catch BEFORE flowOn (fixed repository) -----------------------------------------------

    fun loadOnceFixed(): Job = viewModelScope.launch {
        val profile = repository.profileResult().first().getOrDefault(Profile.PLACEHOLDER)
        show(ProfileUiState.Content(profile))
    }

    fun observeFixed(): Job = viewModelScope.launch {
        repository.profileResult().collect { show(it.toUiState()) }
    }

    fun observeAndPersistFixed(): Job = viewModelScope.launch {
        repository.profileResult().collect { result ->
            show(result.toUiState())
            result.getOrNull()?.let { dao.save(it) }
        }
    }

    private fun Result<Profile>.toUiState(): ProfileUiState = fold(
        onSuccess = { ProfileUiState.Content(it) },
        onFailure = { ProfileUiState.Error(it.message ?: "Something went wrong") },
    )

    /** What `ViewModel.onCleared()` does to `viewModelScope` when the user leaves the screen. */
    fun onCleared() {
        viewModelScope.cancel()
    }
}
