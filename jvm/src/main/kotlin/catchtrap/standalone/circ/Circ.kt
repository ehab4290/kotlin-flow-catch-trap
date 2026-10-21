package catchtrap.standalone.circ

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

fun orderStatus(): Flow<String> = flow {
    emit("PREPARING")
    delay(100)
    throw IllegalStateException("something the data layer didn't expect")
}.flowOn(Dispatchers.IO)

fun main() = runBlocking {
    var crash: Throwable? = null
    val composition = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e -> crash = e })
    composition.launch {
        orderStatus()
            .catch { emit("UNAVAILABLE") }
            .collect { delay(1_000) }
    }.join()

    val e = crash!!
    val suppressed = e.suppressed.single()
    println("crash:               $e")
    println("crash.cause:         ${e.cause}")
    println("crash.suppressed[0]: $suppressed")
    println("suppressed.cause === crash (same object)? ${suppressed.cause === e}")
    println("----- e.printStackTrace() -----")
    e.printStackTrace(System.out)
}
