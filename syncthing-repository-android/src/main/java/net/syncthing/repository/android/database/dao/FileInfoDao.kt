package net.syncthing.repository.android.database.dao

import android.arch.persistence.room.Dao
import android.arch.persistence.room.Insert
import android.arch.persistence.room.OnConflictStrategy
import android.arch.persistence.room.Query
import net.syncthing.repository.android.database.item.FileInfoItem
import net.syncthing.repository.android.database.item.FileInfoLastModified

@Dao
interface FileInfoDao {
    @Query("SELECT * FROM file_info WHERE folder = :folder AND path = :path")
    fun findFileInfo(folder: String, path: String): FileInfoItem?

    @Query("SELECT * FROM file_info WHERE folder = :folder AND path IN (:path)")
    fun findFileInfo(folder: String, path: List<String>): List<FileInfoItem>

    @Query("SELECT last_modified FROM file_info WHERE folder = :folder AND path = :path")
    fun findFileInfoLastModified(folder: String, path: String): FileInfoLastModified?

    @Query("SELECT * FROM file_info WHERE folder = :folder AND path = :path AND is_deleted = 0")
    fun findNotDeletedFileInfo(folder: String, path: String): FileInfoItem?

    @Query("SELECT * FROM file_info WHERE folder = :folder AND parent = :parentPath AND is_deleted = 0")
    fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String): List<FileInfoItem>

    // difference from the old implementation: Using of LIKE instead of REGEXP (Android/ Room/ sqlite does not support it)

    @Query("SELECT COUNT(*) FROM file_info WHERE file_name LIKE :query AND is_deleted = 0")
    fun countFileInfoBySearchTerm(query: String): Long

    @Query("SELECT * FROM file_info WHERE file_name LIKE :query AND is_deleted = 0")
    fun findFileInfoBySearchTerm(query: String): List<FileInfoItem>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    fun updateFileInfo(info: FileInfoItem)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    fun updateFileInfo(info: List<FileInfoItem>)
}
