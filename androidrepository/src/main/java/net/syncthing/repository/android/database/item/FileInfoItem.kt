package net.syncthing.repository.android.database.item

import android.arch.persistence.room.ColumnInfo
import android.arch.persistence.room.Entity
import android.arch.persistence.room.Index
import android.arch.persistence.room.TypeConverters
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.repository.android.database.converters.DateConverter
import net.syncthing.repository.android.database.converters.FileTypeConverter
import java.util.*

@Entity(
        tableName = "file_info",
        primaryKeys = ["folder", "path"],
        indices = [
            Index(value = ["folder", "parent"])
        ]
)
@TypeConverters(
        DateConverter::class,
        FileTypeConverter::class
)
data class FileInfoItem(
        @ColumnInfo(index = true)
        val folder: String,
        val path: String,
        @ColumnInfo(name = "file_name")
        val fileName: String,
        val parent: String,
        val size: Long?,
        val hash: String?,
        @ColumnInfo(name = "last_modified")
        val lastModified: Date,
        @ColumnInfo(name = "file_type")
        val fileType: FileInfo.FileType,
        @ColumnInfo(name = "version_id")
        val versionId: Long,
        @ColumnInfo(name = "version_value")
        val versionValue: Long,
        @ColumnInfo(name = "is_deleted")
        val isDeleted: Boolean
) {
    companion object {
        fun fromNative(item: FileInfo) = FileInfoItem(
                folder = item.folder,
                path = item.path,
                fileName = item.fileName,
                parent = item.parent,
                lastModified = item.lastModified,
                fileType = item.type,
                versionId = item.versionList.last().id,
                versionValue = item.versionList.last().value,
                isDeleted = item.isDeleted,
                size = if (item.isDirectory()) null else item.size,
                hash = if (item.isDirectory()) null else item.hash
        )
    }

    init {
        when (fileType) {
            FileInfo.FileType.DIRECTORY -> {
                if (size != null || hash != null) {
                    throw IllegalArgumentException()
                }
            }
            FileInfo.FileType.FILE -> {
                if (size == null || hash == null) {
                    throw IllegalArgumentException()
                }
            }
        }
    }

    @delegate:Transient
    val native: FileInfo by lazy {
        FileInfo(
                folder = folder,
                type = fileType,
                path = path,
                lastModified = lastModified,
                size = size,
                hash = hash,
                versionList = listOf(FileInfo.Version(
                        id = versionId,
                        value = versionValue
                )),
                isDeleted = isDeleted
        )
    }
}

@TypeConverters(DateConverter::class)
data class FileInfoLastModified(
        @ColumnInfo(name = "last_modified")
        val lastModified: Date
)
