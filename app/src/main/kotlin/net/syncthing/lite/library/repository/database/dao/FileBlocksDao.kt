package net.syncthing.lite.library.repository.database.dao

import android.arch.persistence.room.Dao
import android.arch.persistence.room.Insert
import android.arch.persistence.room.OnConflictStrategy
import android.arch.persistence.room.Query
import net.syncthing.lite.library.repository.database.item.FileBlocksItem

@Dao
interface FileBlocksDao {
    @Query("SELECT * FROM file_blocks WHERE folder = :folder AND path = :path")
    fun findFileBlocks(folder: String, path: String): FileBlocksItem?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    fun mergeBlock(blocksItem: FileBlocksItem)
}
