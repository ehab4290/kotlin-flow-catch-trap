package catchtrap.standalone.savepos

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.IOException
import java.util.concurrent.Executors

val RUNS = System.getenv("RUNS")?.toInt() ?: 200
fun source(): Flow<String> = flow { emit("cached"); delay(5); throw IOException("offline") }
suspend fun save(value: String) = delay(20)   // a suspending Room write

fun main() {
    val main = Executors.newSingleThreadExecutor { Thread(it, "main").apply { isDaemon = true } }.asCoroutineDispatcher()
    val io = Dispatchers.IO
    val cases = listOf<Pair<String, () -> Flow<String>>>(
        "save in the repository, before flowOn"        to { source().onEach { save(it) }.flowOn(io).catch { emit("error") } },
        "save after flowOn, before catch"               to { source().flowOn(io).onEach { save(it) }.catch { emit("error") } },
        "save after catch (or inside collect)"          to { source().flowOn(io).catch { emit("error") }.onEach { save(it) } },
    )
    for ((name, build) in cases) {
        var crashed = 0; var errorShown = 0
        runBlocking(main) {
            repeat(RUNS) {
                try { build().collect { if (it == "error") errorShown++ } } catch (e: IOException) { crashed++ }
            }
        }
        println(String.format("%-44s crashed %3d / $RUNS, error state shown %3d", name, crashed, errorShown))
    }
    main.close()
}
