package net.syncthing.repository.android.database.dao

import android.arch.persistence.room.Dao
import android.arch.persistence.room.Insert
import android.arch.persistence.room.Query
import android.arch.persistence.room.TypeConverters
import net.syncthing.repository.android.database.converters.DateConverter
import net.syncthing.repository.android.database.item.FolderStatsItem
import java.util.*

@Dao
@TypeConverters(DateConverter::class)
interface FolderStatsDao {
    @Query("UPDATE folder_stats SET dir_count = dir_count + :deltaDirCount, file_count = file_count + :deltaFileCount, size = size + :deltaSize, last_update = :lastUpdate WHERE folder = :folder")
    fun updateFolderStats(folder: String, deltaFileCount: Long, deltaDirCount: Long, deltaSize: Long, lastUpdate: Date): Long

    @Insert
    fun insertFolderStats(item: FolderStatsItem)

    @Query("SELECT * FROM folder_stats WHERE folder = :folder")
    fun getFolderStats(folder: String): FolderStatsItem?

    @Query("SELECT * FROM folder_stats")
    fun findAllFolderStats(): List<FolderStatsItem>

    @Query("SELECT * FROM folder_stats WHERE folder = :folder")
    fun findFolderStats(folder: String): FolderStatsItem?
}
