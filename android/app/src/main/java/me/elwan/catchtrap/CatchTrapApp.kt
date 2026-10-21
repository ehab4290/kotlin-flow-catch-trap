package me.elwan.catchtrap

import android.app.Application
import android.util.Log
import me.elwan.catchtrap.data.ProfileApi
import me.elwan.catchtrap.data.ProfileDatabase
import me.elwan.catchtrap.data.ProfileRepository
import java.util.concurrent.ConcurrentHashMap

class CatchTrapApp : Application() {

    private val database by lazy { ProfileDatabase.create(this) }
    private val apis = ConcurrentHashMap<Host, ProfileApi>()

    val repository: ProfileRepository by lazy {
        ProfileRepository(
            dao = database.profileDao(),
            apiFor = { host -> apis.getOrPut(host) { ProfileApi.create(host.baseUrl) } },
        )
    }

    override fun onCreate() {
        super.onCreate()
        logFullCrashTraces()
    }

    /**
     * Android prints no stack trace for a crash whose exception is, or is caused by, an
     * `UnknownHostException`. `android.util.Log.printlns`, which writes the FATAL EXCEPTION entry,
     * drops the trace on purpose "to reduce the amount of log spew" when the network is unavailable.
     * Logcat then shows only the two header lines.
     *
     * This writes the trace as plain text first, then hands the exception to the system's handler,
     * so the app still crashes exactly as before.
     */
    private fun logFullCrashTraces() {
        val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, error ->
            Log.e(TAG, "Uncaught exception on thread \"${thread.name}\":")
            error.stackTraceToString().lineSequence().forEach { line -> Log.e(TAG, line) }
            systemHandler?.uncaughtException(thread, error)
        }
    }

    private companion object {
        const val TAG = "CatchTrap"
    }
}
