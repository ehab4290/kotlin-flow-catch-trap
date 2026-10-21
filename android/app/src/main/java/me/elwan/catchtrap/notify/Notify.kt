package me.elwan.catchtrap.notify

import android.content.Context
import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.Insert
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/*
 * The article's lead example: the chat app's notifications, shown the way Wire shows them
 * (WireNotificationManager.observeLocalNotifications in wire-android). [MessageNotifier] collects the
 * messages that have no notification yet, posts a notification for them, then marks them as notified in
 * the database. Room emits again without them.
 */

/** A message as the sync stores it. `notified` is set once a notification has shown it. */
@Entity(tableName = "messages")
data class MessageEntity(
    @PrimaryKey val id: String,
    val chatId: String,
    val type: String,
    val body: String,
    val notified: Boolean = false,
)

@Dao
interface MessageDao {
    /** The messages with no notification yet. Re-runs the query, and emits again, after every write to the table. */
    @Query("SELECT * FROM messages WHERE notified = 0 ORDER BY id")
    fun observeNotNotified(): Flow<List<MessageEntity>>

    @Insert
    suspend fun insert(message: MessageEntity)

    @Query("UPDATE messages SET notified = 1 WHERE id IN (:ids)")
    suspend fun markNotified(ids: List<String>)

    /** For the tests: how many of these messages are marked as notified. */
    @Query("SELECT COUNT(*) FROM messages WHERE id IN (:ids) AND notified = 1")
    suspend fun countNotified(ids: List<String>): Int
}

@Database(entities = [MessageEntity::class], version = 1, exportSchema = false)
abstract class NotificationDatabase : RoomDatabase() {

    abstract fun messageDao(): MessageDao

    companion object {
        fun inMemory(context: Context): NotificationDatabase =
            Room.inMemoryDatabaseBuilder(context.applicationContext, NotificationDatabase::class.java).build()
    }
}

/** The domain model. */
sealed interface Message {
    val id: String
    data class Text(override val id: String, val text: String) : Message
    data class Photo(override val id: String, val url: String) : Message
}

/** This app version knows these message types and throws on any other, such as a poll added later. */
fun MessageEntity.toDomain(): Message = when (type) {
    "TEXT" -> Message.Text(id, body)
    "PHOTO" -> Message.Photo(id, body)
    else -> error("Unknown message type: $type")
}

/**
 * Stands in for posting through the system's NotificationManager. Posting is instant; then [show] waits
 * [waitMs], as Wire's MessageNotificationManager.handleNotification does with
 * delay(NOTIFICATION_UPDATE_DELAY), 100 ms, "to let notification manager update activeNotifications list".
 */
class ChatNotifications(private val waitMs: Long) {
    val posting = AtomicInteger()
    /** How many times each message was posted. */
    val posted = ConcurrentHashMap<String, Int>()

    suspend fun show(messages: List<Message>) {
        posting.incrementAndGet()
        messages.forEach { posted.merge(it.id, 1, Int::plus) }   // notificationManager.notify(...)
        delay(waitMs)
    }
}

class MessageRepository(
    private val dao: MessageDao,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    /** How often the mapper's error was logged, by the repository's catch or by the per-message try/catch. */
    val mapperFailures = AtomicInteger()

    /** As most repositories are written: map to the domain model, then move the work off the main thread. */
    fun messagesToNotify(): Flow<List<Message>> =
        dao.observeNotNotified()
            .map { rows -> rows.map { it.toDomain() } }
            .filter { it.isNotEmpty() }
            .flowOn(io)

    /** The fix: the same flow with catch before flowOn. */
    fun messagesToNotifyCaughtBeforeFlowOn(): Flow<List<Message>> =
        dao.observeNotNotified()
            .map { rows -> rows.map { it.toDomain() } }
            .filter { it.isNotEmpty() }
            .catch { mapperFailures.incrementAndGet() } // log it
            .flowOn(io)

    /**
     * An error the app can expect, a message type this version doesn't know, handled per message so the
     * notifications keep going. catch before flowOn stays, for everything else. The unknown message gets
     * no notification and stays in the query.
     */
    fun messagesToNotifyPerMessage(): Flow<List<Message>> =
        dao.observeNotNotified()
            .map { rows ->
                rows.mapNotNull { row ->
                    try { row.toDomain() } catch (e: IllegalStateException) { mapperFailures.incrementAndGet(); null } // log it
                }
            }
            .filter { it.isNotEmpty() }
            .catch { mapperFailures.incrementAndGet() } // log it
            .flowOn(io)

    suspend fun markNotified(ids: List<String>) = dao.markNotified(ids)   // Room emits again, without these messages
}

/**
 * Posts a notification for new messages, then marks them as notified, like Wire's
 * WireNotificationManager.observeLocalNotifications. The app runs [observe] once, in an app-wide scope
 * with no exception handler, as Wire does with CoroutineScope(SupervisorJob() + Dispatchers.Default). The
 * catch is the observer's fallback for anything the data layer throws; it sits after the repository's
 * flowOn. The other functions are the ways out the tests compare.
 */
class MessageNotifier(
    private val repository: MessageRepository,
    private val notifications: ChatNotifications,
    private val toNotify: Flow<List<Message>> = repository.messagesToNotify(),
) {
    /** Runs in which this observer's catch block ran. */
    val fallbackRan = AtomicInteger()

    suspend fun observe() {
        toNotify
            .catch { fallbackRan.incrementAndGet() } // log it
            .collect { messages ->
                notifications.show(messages)
                repository.markNotified(messages.map { it.id })
            }
    }

    /** collectLatest instead of collect: a newer list cancels the work on the older one. */
    suspend fun observeWithCollectLatest() {
        toNotify
            .catch { fallbackRan.incrementAndGet() } // log it
            .collectLatest { messages ->
                notifications.show(messages)
                repository.markNotified(messages.map { it.id })
            }
    }

    /** The work in onEach, before catch. Nothing after catch waits. */
    suspend fun observeInOnEach() {
        toNotify
            .onEach { messages ->
                notifications.show(messages)
                repository.markNotified(messages.map { it.id })
            }
            .catch { fallbackRan.incrementAndGet() } // log it
            .collect()
    }

    /** The same, with flowOn after onEach, so the work runs on [io]. */
    suspend fun observeInOnEachWithFlowOn(io: CoroutineDispatcher = Dispatchers.IO) {
        toNotify
            .onEach { messages ->
                notifications.show(messages)
                repository.markNotified(messages.map { it.id })
            }
            .flowOn(io)
            .catch { fallbackRan.incrementAndGet() } // log it
            .collect()
    }
}
