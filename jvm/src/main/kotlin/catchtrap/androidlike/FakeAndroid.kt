package catchtrap.androidlike

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.io.Closeable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext

/**
 * A single thread named "main" that behaves like `Dispatchers.Main.immediate`:
 * code already running on the main thread is not re-posted, it runs in place.
 */
class FakeMainDispatcher : CoroutineDispatcher(), Closeable {

    @Volatile
    private var mainThread: Thread? = null

    private val executor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "main").apply { isDaemon = true }.also { mainThread = it }
    }

    override fun isDispatchNeeded(context: CoroutineContext): Boolean =
        Thread.currentThread() !== mainThread

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        executor.execute(block)
    }

    override fun close() {
        executor.shutdown()
    }

    override fun toString(): String = "Dispatchers.Main.immediate (fake)"
}

/**
 * The two pieces of Android that matter for this experiment, on the plain JVM.
 *
 * 1. [main] plays the main thread.
 * 2. The default uncaught-exception handler plays the crash. `viewModelScope` has no
 *    `CoroutineExceptionHandler`, so a coroutine launched in it that fails with an exception hands
 *    that exception to `Thread.getDefaultUncaughtExceptionHandler()`. On Android that handler logs
 *    "FATAL EXCEPTION: main" and kills the process. Here it records the exception in [crashes].
 */
class FakeAndroid : Closeable {

    val main = FakeMainDispatcher()

    /** Every exception that would have crashed the app. */
    val crashes = CopyOnWriteArrayList<Throwable>()

    private val previousHandler = Thread.getDefaultUncaughtExceptionHandler()

    init {
        Thread.setDefaultUncaughtExceptionHandler { _, e -> crashes += e }
    }

    /** Same recipe as androidx's `viewModelScope`: SupervisorJob + Dispatchers.Main.immediate, no handler. */
    fun newViewModelScope(): CoroutineScope = CoroutineScope(SupervisorJob() + main)

    /** Runs [block] on the main thread and waits for it, like a UI callback would. */
    fun <T> onMainThread(block: suspend CoroutineScope.() -> T): T = runBlocking(main) { block() }

    override fun close() {
        Thread.setDefaultUncaughtExceptionHandler(previousHandler)
        main.close()
    }
}
