package catchtrap.standalone.ways

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import catchtrap.fixes.catchAware

fun main() {
    val main = newFakeMainDispatcher()
    val msgs = sortedMapOf<String, Int>()
    runBlocking(main) {
        for ((name, op) in listOf<Pair<String, (Flow<String>) -> Flow<String>>>(
            "transform" to { f -> f.transform { emit(it) } },
            "flow wrapper" to { f -> flow { f.collect { emit(it) } } })) repeat((System.getenv("RUNS")?.toInt() ?: 200)) {
            try {
                op(source(Failure.AFTER_SUSPENDING).flowOn(Dispatchers.IO).catchAware { emit("fallback") }).collect { delay(10) }
                msgs.merge("$name: completed normally", 1, Int::plus)
            } catch (e: IllegalStateException) {
                msgs.merge("$name: ISE \"" + e.message!!.lineSequence().first() + "\"", 1, Int::plus)
            }
        }
    }
    for (name in listOf("transform", "flow wrapper")) {
        val ise = msgs.filterKeys { it.startsWith("$name: ISE") }
        println("%-13s ISE 'Flow exception transparency is violated' %d, other ISE %d, completed normally %d".format(name,
            ise.filterKeys { it.contains("Flow exception transparency is violated") }.values.sum(),
            ise.filterKeys { !it.contains("Flow exception transparency is violated") }.values.sum(),
            msgs["$name: completed normally"] ?: 0))
    }
    main.close()
}
