package me.elwan.catchtrap

import android.database.Cursor
import android.os.CancellationSignal
import android.os.Looper
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.room.Room
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.sqlite.db.SupportSQLiteOpenHelper
import androidx.sqlite.db.SupportSQLiteQuery
import androidx.sqlite.db.SupportSQLiteStatement
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import me.elwan.catchtrap.data.ProfileApi
import me.elwan.catchtrap.data.ProfileDatabase
import me.elwan.catchtrap.data.ProfileEntity
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import retrofit2.Retrofit
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * "Room's suspend DAO functions and Retrofit's suspend functions already move their work off the
 * main thread. If everything in the builder is main-safe, delete flowOn, and the catch in the
 * ViewModel works as written."
 *
 * Every call below is made from the main thread, with Room's and Retrofit's default setup:
 *  - Room: the SQL itself is recorded with its thread, by a thin wrapper around the framework's SQLite helper.
 *  - Retrofit: an OkHttp interceptor records the thread the request runs on, and answers it without a network.
 * Each test also has a control that does run on the main thread, to show the recorder would notice.
 *
 *     ./gradlew connectedDebugAndroidTest -Pandroid.testInstrumentationRunnerArguments.class=me.elwan.catchtrap.MainSafeCallsTest
 *     adb logcat -s CatchTrap
 */
@RunWith(AndroidJUnit4::class)
class MainSafeCallsTest {

    private val context = InstrumentationRegistry.getInstrumentation().targetContext

    private class Recorded(val what: String, val onMain: Boolean, val thread: String)

    private fun onMain() = Looper.myLooper() == Looper.getMainLooper()

    // ---- Room -------------------------------------------------------------------------------------

    private class RecordingDatabase(
        private val db: SupportSQLiteDatabase,
        private val record: (String) -> Unit,
    ) : SupportSQLiteDatabase by db {
        override fun compileStatement(sql: String): SupportSQLiteStatement { record(sql); return db.compileStatement(sql) }
        override fun query(query: String): Cursor { record(query); return db.query(query) }
        override fun query(query: String, bindArgs: Array<out Any?>): Cursor { record(query); return db.query(query, bindArgs) }
        override fun query(query: SupportSQLiteQuery): Cursor { record(query.sql); return db.query(query) }
        override fun query(query: SupportSQLiteQuery, cancellationSignal: CancellationSignal?): Cursor {
            record(query.sql); return db.query(query, cancellationSignal)
        }
        override fun execSQL(sql: String) { record(sql); db.execSQL(sql) }
        override fun execSQL(sql: String, bindArgs: Array<out Any?>) { record(sql); db.execSQL(sql, bindArgs) }
    }

    private class RecordingFactory(private val record: (String) -> Unit) : SupportSQLiteOpenHelper.Factory {
        private val framework = FrameworkSQLiteOpenHelperFactory()
        override fun create(configuration: SupportSQLiteOpenHelper.Configuration): SupportSQLiteOpenHelper {
            val helper = framework.create(configuration)
            return object : SupportSQLiteOpenHelper by helper {
                override val writableDatabase: SupportSQLiteDatabase get() = RecordingDatabase(helper.writableDatabase, record)
                override val readableDatabase: SupportSQLiteDatabase get() = RecordingDatabase(helper.readableDatabase, record)
            }
        }
    }

    @Test
    fun roomSuspendDaoFunctions_runTheirSqlOffTheMainThread() {
        val sql = CopyOnWriteArrayList<Recorded>()
        val db = Room.inMemoryDatabaseBuilder(context, ProfileDatabase::class.java)
            .openHelperFactory(RecordingFactory { s -> sql += Recorded(s, onMain(), Thread.currentThread().name) })
            .build()
        try {
            var calledFromMain = true
            runBlocking(Dispatchers.Main) {
                repeat(RUNS) {
                    if (!onMain()) calledFromMain = false
                    db.profileDao().save(ProfileEntity(name = "run $it", updatedAt = it.toLong()))  // suspend @Upsert
                    db.profileDao().cachedProfileSuspending()                                       // suspend @Query
                }
            }
            val daoSql = sql.filter { (it.what.startsWith("INSERT") || it.what.startsWith("UPDATE") || it.what.startsWith("SELECT")) && "profile" in it.what }
            val daoSqlOnMain = daoSql.count { it.onMain }

            // Control: the same recorder, SQL run directly on the main thread.
            sql.clear()
            runBlocking(Dispatchers.Main) { db.openHelper.writableDatabase.query("SELECT * FROM profile").close() }
            val controlOnMain = sql.count { it.onMain }

            Log.i(TAG, "[Room suspend DAO, called from main] $RUNS saves + $RUNS reads: ${daoSql.size} statements, " +
                "$daoSqlOnMain on the main thread, threads ${daoSql.map { it.thread }.distinct()} | " +
                "control, SQL run directly on main: $controlOnMain/${sql.size} on the main thread")
            assertTrue(calledFromMain)
            assertTrue("no DAO SQL was recorded", daoSql.size >= 2 * RUNS)
            assertEquals(0, daoSqlOnMain)
            assertTrue("the recorder did not see the main thread", controlOnMain > 0)
        } finally {
            db.close()
        }
    }

    // ---- Retrofit ---------------------------------------------------------------------------------

    private fun api(requests: MutableList<Recorded>, fail: Boolean): ProfileApi {
        val client = OkHttpClient.Builder()
            .addInterceptor { chain ->
                requests += Recorded(chain.request().url.toString(), onMain(), Thread.currentThread().name)
                if (fail) throw IOException("network down")
                Response.Builder()
                    .request(chain.request())
                    .protocol(Protocol.HTTP_1_1)
                    .code(200).message("OK")
                    .body("""{"name":"Octocat"}""".toResponseBody("application/json".toMediaType()))
                    .build()
            }
            .build()
        return Retrofit.Builder().baseUrl("http://example.invalid/").client(client).build().create(ProfileApi::class.java)
    }

    @Test
    fun retrofitSuspendFunctions_runTheRequestOffTheMainThread() {
        val requests = CopyOnWriteArrayList<Recorded>()
        val api = api(requests, fail = false)
        var calledFromMain = true
        runBlocking(Dispatchers.Main) {
            repeat(RUNS) {
                if (!onMain()) calledFromMain = false
                api.fetchProfile().close()                       // suspend fun
            }
        }
        val suspendingOnMain = requests.count { it.onMain }
        val threads = requests.map { it.thread }.distinct()

        // Control: the blocking Call.execute(), made on the main thread, runs the request right there.
        requests.clear()
        runBlocking(Dispatchers.Main) { api.fetchProfileBlocking().execute().body()?.close() }
        val controlOnMain = requests.count { it.onMain }

        Log.i(TAG, "[Retrofit suspend fun, called from main] ${RUNS} requests, $suspendingOnMain on the main thread, " +
            "threads $threads | control, Call.execute() on main: $controlOnMain/${requests.size} on the main thread")
        assertTrue(calledFromMain)
        assertEquals(0, suspendingOnMain)
        assertEquals(1, controlOnMain)
    }

    // ---- The whole chain: Room + Retrofit, no flowOn, catch in the ViewModel -----------------------

    class ProbeViewModel : ViewModel()

    private fun runChain(withFlowOn: Boolean): String {
        val sql = CopyOnWriteArrayList<Recorded>()
        val requests = CopyOnWriteArrayList<Recorded>()
        val db = Room.inMemoryDatabaseBuilder(context, ProfileDatabase::class.java)
            .openHelperFactory(RecordingFactory { s -> sql += Recorded(s, onMain(), Thread.currentThread().name) })
            .build()
        val api = api(requests, fail = true)
        runBlocking { db.profileDao().save(ProfileEntity(name = "Octocat (cached)", updatedAt = 0)) }

        val uncaught = CopyOnWriteArrayList<Throwable>()
        val systemHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
        var crashes = 0
        var fallbackShown = 0
        try {
            repeat(RUNS) { run ->
                uncaught.clear()
                val vm = ProbeViewModel()
                val done = CountDownLatch(1)
                var sawFallback = false
                val source = flow {
                    emit(db.profileDao().cachedProfileSuspending()?.name ?: "none")  // Room, suspend
                    emit(api.fetchProfile().string())                               // Retrofit, suspend: fails
                }
                vm.viewModelScope.launch {
                    (if (withFlowOn) source.flowOn(Dispatchers.IO) else source)
                        .catch { emit("fallback") }
                        .collect { value ->
                            if (value == "fallback") sawFallback = true
                            delay(100) // the collector is busy, like showSnackbar()
                        }
                }.invokeOnCompletion { done.countDown() }
                assertTrue("run $run did not finish", done.await(5, TimeUnit.SECONDS))
                Thread.sleep(100) // let a crash reach the handler
                vm.viewModelScope.cancel()
                if (uncaught.isNotEmpty()) crashes++
                if (sawFallback) fallbackShown++
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(systemHandler)
            db.close()
        }
        val label = if (withFlowOn) "flowOn(IO) in the repository" else "no flowOn"
        val daoOnMain = sql.count { it.onMain && it.what.startsWith("SELECT") && "profile" in it.what }
        val httpOnMain = requests.count { it.onMain }
        val summary = "[$label, catch in the ViewModel, suspending collector] crashed $crashes/$RUNS, fallback shown $fallbackShown/$RUNS, " +
            "all runs: Room SELECT on main $daoOnMain, HTTP on main $httpOnMain"
        Log.i(TAG, summary)
        return summary
    }

    @Test
    fun noFlowOn_mainSafeCalls_catchInViewModelWorks() {
        val noFlowOn = runChain(withFlowOn = false)
        assertTrue(noFlowOn, noFlowOn.contains("crashed 0/$RUNS, fallback shown $RUNS/$RUNS"))
        assertTrue(noFlowOn, noFlowOn.endsWith("all runs: Room SELECT on main 0, HTTP on main 0"))
    }

    /** The same chain with flowOn: shows the chain above is one the trap does hit. Asserts nothing. */
    @Test
    fun withFlowOn_control() {
        runChain(withFlowOn = true)
    }

    private companion object {
        const val TAG = "CatchTrap"
        /** Runs per row: `-e runs 200` on the command line, 40 if not given. */
        val RUNS: Int = InstrumentationRegistry.getArguments().getString("runs")?.toIntOrNull() ?: 40
    }
}
