package net.syncthing.repository.android.database

import android.arch.persistence.room.Database
import android.arch.persistence.room.Room
import android.arch.persistence.room.RoomDatabase
import android.content.Context
import net.syncthing.repository.android.database.dao.*
import net.syncthing.repository.android.database.item.*

@Database(
        version = 1,
        entities = [
            FileBlocksItem::class,
            FileInfoItem::class,
            FolderIndexInfoItem::class,
            FolderStatsItem::class,
            IndexSequenceItem::class
        ]
)
abstract class RepositoryDatabase: RoomDatabase() {
    companion object {
        private var instance: RepositoryDatabase? = null
        private val lock = Object()

        fun createInstance(context: Context, name: String) = Room.databaseBuilder(
                context.applicationContext,
                RepositoryDatabase::class.java,
                name
        ).build()

        fun createInMemoryInstance(context: Context) = Room.inMemoryDatabaseBuilder(
                context.applicationContext,
                RepositoryDatabase::class.java
        ).build()

        fun with(context: Context): RepositoryDatabase {
            if (instance == null) {
                synchronized (lock) {
                    if (instance == null) {
                        instance = createInstance(context, "repository_database")
                    }
                }
            }

            return instance!!
        }
    }

    abstract fun fileInfo(): FileInfoDao
    abstract fun fileBlocks(): FileBlocksDao
    abstract fun folderStats(): FolderStatsDao
    abstract fun folderIndexInfo(): FolderIndexInfoDao
    abstract fun indexSequence(): IndexSequenceDao
}
