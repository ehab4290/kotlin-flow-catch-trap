package catchtrap.standalone.shapes

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

class NetworkError : RuntimeException("network down")

fun show(label: String, e: Throwable?) {
    println("=== $label")
    if (e == null) { println("no crash"); return }
    val sw = java.io.StringWriter(); e.printStackTrace(java.io.PrintWriter(sw))
    sw.toString().lines().filter { it.isNotBlank() && !it.trimStart().startsWith("at ") && !it.trimStart().startsWith("...") }
        .forEach { println(it.replace("\t", "    ").take(120)) }
}

suspend fun crashOf(block: suspend CoroutineScope.() -> Unit): Throwable? {
    var crash: Throwable? = null
    val scope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e -> crash = e })
    scope.launch(block = block).join()
    return crash
}

fun main() = runBlocking {
    // Case 1: collector suspended when the source fails
    show("1 collector suspended", crashOf {
        flow { emit(1); delay(5); throw NetworkError() }.flowOn(Dispatchers.IO)
            .catch { emit(-1) }.collect { delay(20) }
    })
    // Case 2: early stop (first) with a blocking source
    show("2 first() + blocking source", crashOf {
        flow { emit(1); Thread.sleep(5); throw NetworkError() }.flowOn(Dispatchers.IO)
            .catch { emit(-1) }.first()
    })
    // Case 3: user leaves the screen while collector works; blocking source fails afterwards
    show("3 scope cancelled, blocking source", crashOf {
        val job = coroutineContext[Job]!!
        launch { delay(5); job.cancel() }
        flow { emit(1); Thread.sleep(30); throw NetworkError() }.flowOn(Dispatchers.IO)
            .catch { emit(-1) }.collect { delay(1_000) }
    })
    // Case 4: collector throws its own exception
    show("4 collector throws", crashOf {
        flow { emit(1); throw NetworkError() }.flowOn(Dispatchers.IO)
            .catch { emit(-1) }.collect { Thread.sleep(5); error("bug in collector") }
    })
}
