package net.syncthing.repository.android.database.item

import android.arch.persistence.room.ColumnInfo
import android.arch.persistence.room.Entity
import android.arch.persistence.room.TypeConverters
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.repository.android.database.converters.BlockInfoListConverter

@Entity(
        tableName = "file_blocks",
        primaryKeys = ["folder", "path"]
)
@TypeConverters(BlockInfoListConverter::class)
data class FileBlocksItem(
        val folder: String,
        val path: String,
        val hash: String,
        val size: Long,
        @ColumnInfo(typeAffinity = ColumnInfo.BLOB)
        val blocks: List<BlockInfo>
) {
    companion object {
        fun fromNative(block: FileBlocks) = FileBlocksItem(
                folder = block.folder,
                path = block.path,
                hash = block.hash,
                size = block.size,
                blocks = block.blocks
        )
    }

    @delegate:Transient
    val native: FileBlocks by lazy {
        FileBlocks(
                folder = folder,
                path = path,
                blocks = blocks
        )
    }
}
