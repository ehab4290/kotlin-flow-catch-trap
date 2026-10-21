package me.elwan.catchtrap.data

import okhttp3.OkHttpClient
import okhttp3.ResponseBody
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.http.GET
import java.util.concurrent.TimeUnit

interface ProfileApi {

    /** A suspending request. Cancelling the coroutine cancels the HTTP call. */
    @GET("users/octocat")
    suspend fun fetchProfile(): ResponseBody

    /** The same request as a blocking call. */
    @GET("users/octocat")
    fun fetchProfileBlocking(): Call<ResponseBody>

    companion object {
        fun create(baseUrl: String): ProfileApi {
            val client = OkHttpClient.Builder()
                .callTimeout(10, TimeUnit.SECONDS)
                .build()
            return Retrofit.Builder()
                .baseUrl(baseUrl)
                .client(client)
                .build()
                .create(ProfileApi::class.java)
        }
    }
}
