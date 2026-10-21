package me.elwan.catchtrap.data

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import me.elwan.catchtrap.CallStyle
import me.elwan.catchtrap.Host
import me.elwan.catchtrap.NetworkSetup
import me.elwan.catchtrap.Profile
import okhttp3.ResponseBody
import org.json.JSONObject
import retrofit2.HttpException

class ProfileRepository(
    private val dao: ProfileDao,
    private val apiFor: (Host) -> ProfileApi,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    /** The cached row from Room, then a fresh copy from the network. */
    private fun cachedThenFresh(setup: NetworkSetup): Flow<Profile> = flow {
        dao.cachedProfile()?.let { emit(Profile(name = it.name, origin = Profile.Origin.CACHE)) }
        emit(fetch(setup))
    }

    /**
     * The shape from the official Android guide: `flowOn` inside the repository,
     * error handling left to whoever collects.
     */
    fun profile(setup: NetworkSetup): Flow<Profile> = cachedThenFresh(setup).flowOn(io)

    /** The fix: the error becomes a value before it crosses `flowOn`. */
    fun profileResult(setup: NetworkSetup): Flow<Result<Profile>> =
        cachedThenFresh(setup)
            .map { Result.success(it) }
            .catch { emit(Result.failure(it)) }
            .flowOn(io)

    /** A suspending Room write. */
    suspend fun save(profile: Profile) {
        dao.save(ProfileEntity(name = profile.name, updatedAt = System.currentTimeMillis()))
        delay(1_000) // the write takes 1 s, so the collector is suspended meanwhile
    }

    private suspend fun fetch(setup: NetworkSetup): Profile {
        val api = apiFor(setup.host)
        val body: ResponseBody = when (setup.callStyle) {
            CallStyle.SUSPENDING -> api.fetchProfile()
            CallStyle.BLOCKING -> {
                val response = api.fetchProfileBlocking().execute()
                if (!response.isSuccessful) throw HttpException(response)
                response.body() ?: throw HttpException(response)
            }
        }
        val name = body.use { JSONObject(it.string()).optString("name", "(no name)") }
        return Profile(name = name, origin = Profile.Origin.NETWORK)
    }
}
