package me.elwan.catchtrap.orders

import android.content.Context
import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.Upsert
import kotlinx.coroutines.flow.Flow

/**
 * An order as the sync worker stores it. The row is flat, so driverName is nullable: there is no
 * driver while the order is PREPARING.
 */
@Entity(tableName = "orders")
data class OrderEntity(
    @PrimaryKey val id: String,
    val status: String,
    val driverName: String? = null,
)

@Dao
interface OrderDao {
    /** Re-runs the query, and emits again, after every write to the table. */
    @Query("SELECT * FROM orders WHERE id = :id")
    fun observeOrder(id: String): Flow<OrderEntity?>

    @Upsert
    suspend fun upsert(order: OrderEntity)
}

@Database(entities = [OrderEntity::class], version = 1, exportSchema = false)
abstract class OrderDatabase : RoomDatabase() {

    abstract fun orderDao(): OrderDao

    companion object {
        fun inMemory(context: Context): OrderDatabase =
            Room.inMemoryDatabaseBuilder(context.applicationContext, OrderDatabase::class.java).build()
    }
}
