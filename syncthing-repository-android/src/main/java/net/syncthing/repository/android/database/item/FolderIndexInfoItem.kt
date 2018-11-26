package net.syncthing.repository.android.database.item

import android.arch.persistence.room.ColumnInfo
import android.arch.persistence.room.Entity
import net.syncthing.java.core.beans.IndexInfo

@Entity(
        primaryKeys = ["folder", "device_id"],
        tableName = "folder_index_info"
)
data class FolderIndexInfoItem(
        val folder: String,
        @ColumnInfo(name = "device_id")
        val deviceId: String,
        @ColumnInfo(name = "index_id")
        val indexId: Long,
        @ColumnInfo(name = "local_sequence")
        val localSequence: Long,
        @ColumnInfo(name = "max_sequence")
        val maxSequence: Long
) {
    companion object {
        fun fromNative(item: IndexInfo) = FolderIndexInfoItem(
                folder = item.folderId,
                deviceId = item.deviceId,
                indexId = item.indexId,
                localSequence = item.localSequence,
                maxSequence = item.maxSequence
        )
    }

    @delegate:Transient
    val native: IndexInfo by lazy {
        IndexInfo(
                folderId = folder,
                deviceId = deviceId,
                indexId = indexId,
                localSequence = localSequence,
                maxSequence = maxSequence
        )
    }
}
