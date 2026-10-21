package me.elwan.catchtrap

data class Profile(val name: String, val origin: Origin) {
    enum class Origin { CACHE, NETWORK, PLACEHOLDER }

    companion object {
        val PLACEHOLDER = Profile(name = "Guest", origin = Origin.PLACEHOLDER)
    }
}

sealed interface ProfileUiState {
    data class Content(val profile: Profile) : ProfileUiState
    data class Error(val message: String) : ProfileUiState
}

/** Where the network request goes. */
enum class Host(val label: String, val baseUrl: String) {
    /** A real endpoint. It fails only when the device is offline, so use airplane mode. */
    REAL("Real host (turn on airplane mode to make it fail)", "https://api.github.com/"),

    /** `.invalid` is reserved by RFC 2606 and never resolves: a real DNS failure, online or offline. */
    UNRESOLVABLE("Unresolvable host (always fails)", "https://profile-api.invalid/"),
}

/** How the repository makes the request. */
enum class CallStyle(val label: String) {
    /** Cancelling the coroutine cancels the HTTP call. */
    SUSPENDING("Retrofit suspend fun"),

    /** Nothing can interrupt `execute()`: it returns or throws when the network stack is done. */
    BLOCKING("Retrofit Call.execute()"),
}

data class NetworkSetup(val host: Host, val callStyle: CallStyle)

/** One thing the ViewModel can do with the repository. */
enum class Scenario(val title: String, val fixed: Boolean) {
    LOAD_ONCE("loadOnce(): catch { }.first()", fixed = false),
    OBSERVE("observe(): catch { }.collect { show(it) }", fixed = false),
    OBSERVE_AND_PERSIST("observeAndPersist(): collect { show(it); save(it) }", fixed = false),
    LOAD_ONCE_FIXED("loadOnce()", fixed = true),
    OBSERVE_FIXED("observe()", fixed = true),
    OBSERVE_AND_PERSIST_FIXED("observeAndPersist()", fixed = true),
}
