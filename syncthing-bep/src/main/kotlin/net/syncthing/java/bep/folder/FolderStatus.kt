package net.syncthing.java.bep.folder

import net.syncthing.java.bep.utils.longMaxBy
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.java.core.beans.IndexInfo

data class FolderStatus(
        val info: FolderInfo,
        val stats: FolderStats,
        val indexInfo: List<IndexInfo>
) {
    companion object {
        fun createDummy(folder: String) = FolderStatus(
                info = FolderInfo(
                        folder,
                        folder,
                        deviceIdBlacklist = emptySet(),
                        deviceIdWhitelist = emptySet(),
                        ignoredDeviceIdList = emptySet()
                ),
                stats = FolderStats.createDummy(folder),
                indexInfo = emptyList()
        )
    }

    val missingIndexUpdates: Long by lazy {
        Math.max(
                0,
                indexInfo.longMaxBy ({ it.maxSequence }, 0) -
                        indexInfo.longMaxBy ({ it.localSequence }, 0)

        )
    }
}
