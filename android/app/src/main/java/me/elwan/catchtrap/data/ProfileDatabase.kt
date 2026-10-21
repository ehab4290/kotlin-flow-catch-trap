package me.elwan.catchtrap.data

import android.content.Context
import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.Upsert
import androidx.sqlite.db.SupportSQLiteDatabase

@Entity(tableName = "profile")
data class ProfileEntity(
    @PrimaryKey val id: Int = 1,
    val name: String,
    val updatedAt: Long,
)

@Dao
interface ProfileDao {
    /** A blocking read. The repository calls it on its io dispatcher. */
    @Query("SELECT * FROM profile WHERE id = 1")
    fun cachedProfile(): ProfileEntity?

    /** The same read as a suspend function. */
    @Query("SELECT * FROM profile WHERE id = 1")
    suspend fun cachedProfileSuspending(): ProfileEntity?

    /** A suspending write. */
    @Upsert
    suspend fun save(profile: ProfileEntity)
}

@Database(entities = [ProfileEntity::class], version = 1, exportSchema = false)
abstract class ProfileDatabase : RoomDatabase() {

    abstract fun profileDao(): ProfileDao

    companion object {
        /** A database file that starts with one cached profile, as if an earlier session had saved it. */
        fun create(context: Context, name: String = "profile.db"): ProfileDatabase =
            Room.databaseBuilder(context.applicationContext, ProfileDatabase::class.java, name)
                .addCallback(SeedCachedProfile)
                .build()
    }
}

private object SeedCachedProfile : RoomDatabase.Callback() {
    override fun onCreate(db: SupportSQLiteDatabase) {
        db.execSQL("INSERT INTO profile (id, name, updatedAt) VALUES (1, 'Octocat (cached)', 0)")
    }
}
