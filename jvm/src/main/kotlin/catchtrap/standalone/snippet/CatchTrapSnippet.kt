package catchtrap.standalone.snippet

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

// Repository: emits the messages to notify, then fails on a message type this version doesn't know.
fun messagesToNotify(): Flow<List<String>> = flow {
    emit(listOf("Are you coming?"))
    delay(30) // a new message arrives while the notification waits
    throw IllegalStateException("Unknown message type: POLL")
}.flowOn(Dispatchers.IO)

fun main() = runBlocking {
    // The app's scope, as in Wire. With no handler, an uncaught exception here crashes the app;
    // this handler only prints it.
    val appScope = CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, e ->
        print("CRASH: ")
        e.printStackTrace(System.out)
    })
    appScope.launch {
        messagesToNotify()
            .catch { println("Logged: ${it.message}") }
            .collect { messages ->
                println("Notifying about ${messages.size} message(s)")
                delay(100) // notifications.show(messages): posts, then waits, as Wire's does
            }
    }.join()
}
