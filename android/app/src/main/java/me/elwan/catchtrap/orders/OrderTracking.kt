package me.elwan.catchtrap.orders

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.stateIn
import java.util.concurrent.atomic.AtomicInteger

/** The domain model: each status carries what it needs. */
sealed interface Order {
    data object Preparing : Order
    data class OnTheWay(val driverName: String) : Order
    data object Delivered : Order
    data object Unknown : Order
    /** What the catch blocks emit. */
    data object Unavailable : Order
}

/**
 * ON_THE_WAY requires a driver. An order handed to a partner courier arrives ON_THE_WAY with no
 * driver, and app versions written before that backend change throw here. Unknown statuses are handled.
 */
fun OrderEntity.toDomain(): Order = when (status) {
    "PREPARING" -> Order.Preparing
    "ON_THE_WAY" -> Order.OnTheWay(requireNotNull(driverName) { "ON_THE_WAY without a driver" })
    "DELIVERED" -> Order.Delivered
    else -> Order.Unknown
}

class OrderRepository(
    private val dao: OrderDao,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    /** How often the catch in [ordersCaughtBeforeFlowOn] ran, that is, how often the mapper threw. */
    val mapperFailures = AtomicInteger()

    /** As most repositories are written: map to the domain model, then move the work off the main thread. */
    fun orders(id: String): Flow<Order> = orders(dao.observeOrder(id))

    /** The same chain on any source of rows. The tests use it to send many updates at once. */
    fun orders(rows: Flow<OrderEntity?>): Flow<Order> =
        rows
            .filterNotNull()
            .map { it.toDomain() }
            .flowOn(io)

    /** The fix: the same flow with catch before flowOn. */
    fun ordersCaughtBeforeFlowOn(id: String): Flow<Order> =
        dao.observeOrder(id)
            .filterNotNull()
            .map { it.toDomain() }
            .catch { mapperFailures.incrementAndGet(); emit(Order.Unavailable) }
            .flowOn(io)
}

/** How the ViewModel hands each update to the UI. */
enum class MessageStyle(val label: String) {
    RENDEZVOUS_CHANNEL("Channel<String>()"),
    BUFFERED_CHANNEL("Channel<String>(BUFFERED)"),
    SHARED_FLOW("MutableSharedFlow<String>()"),
    SHARE_IN("shareIn"),
    STATE_IN("stateIn"),
    MUTABLE_STATE("MutableStateFlow set by the ViewModel"),
}

/**
 * One snackbar message per order update. The catch is the ViewModel's fallback for anything the data
 * layer throws; it sits after the repository's flowOn.
 */
class OrderViewModel(
    orders: Flow<Order>,
    style: MessageStyle,
    /** Tests only: called for each update from the repository, before the ViewModel hands it over. */
    private val onReceived: (() -> Unit)? = null,
) : ViewModel() {

    /** Runs in which this ViewModel's catch block ran. */
    val fallbackRan = AtomicInteger()

    private val channel = Channel<String>(if (style == MessageStyle.BUFFERED_CHANNEL) Channel.BUFFERED else Channel.RENDEZVOUS)
    private val shared = MutableSharedFlow<String>()

    private val safeOrders = orders
        .onEach { onReceived?.invoke() }
        .catch { fallbackRan.incrementAndGet(); emit(Order.Unavailable) }

    /** What the UI collects and shows in a snackbar, one message at a time. */
    val messages: Flow<String> = when (style) {
        MessageStyle.SHARED_FLOW -> shared
        MessageStyle.SHARE_IN -> safeOrders.map { "Your order: $it" }.shareIn(viewModelScope, SharingStarted.Eagerly)
        else -> channel.receiveAsFlow()
    }

    var state: StateFlow<Order>? = null
        private set

    init {
        when (style) {
            MessageStyle.SHARE_IN -> Unit // shareIn above does the collecting
            MessageStyle.STATE_IN ->
                state = safeOrders.stateIn(viewModelScope, SharingStarted.Eagerly, Order.Unknown)
            MessageStyle.MUTABLE_STATE -> {
                val mutable = MutableStateFlow<Order>(Order.Unknown)
                state = mutable
                safeOrders.onEach { mutable.value = it }.launchIn(viewModelScope)
            }
            MessageStyle.SHARED_FLOW ->
                safeOrders.onEach { shared.emit("Your order: $it") }.launchIn(viewModelScope)
            else ->
                safeOrders.onEach { channel.send("Your order: $it") }.launchIn(viewModelScope)
        }
    }
}
