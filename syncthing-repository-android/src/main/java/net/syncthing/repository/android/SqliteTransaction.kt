package net.syncthing.repository.android

import net.syncthing.java.core.beans.*
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.repository.android.database.RepositoryDatabase
import net.syncthing.repository.android.database.item.*
import java.util.*

class SqliteTransaction(
        private val database: RepositoryDatabase,
        private val threadId: Long,
        private val clearTempStorageHook: () -> Unit
): IndexTransaction {
    private var finished = false

    private fun assertAllowed() {
        if (finished) {
            throw IllegalStateException("tried to use a transaction which is already done")
        }

        if (Thread.currentThread().id != threadId) {
            throw IllegalStateException("tried to access the transaction from an other Thread")
        }
    }

    fun markFinished() {
        finished = true
    }

    private fun <T> runIfAllowed(block: () -> T): T {
        assertAllowed()

        return block()
    }

    // FileInfo
    override fun findFileInfo(folder: String, path: String) = runIfAllowed {
        database.fileInfo().findFileInfo(folder, path)?.native
    }

    override fun findFileInfo(folder: String, path: List<String>): Map<String, FileInfo> = runIfAllowed {
        database.fileInfo().findFileInfo(folder, path)
                .map { it.native }
                .associateBy { it.path }
    }

    override fun findFileInfoBySearchTerm(query: String) = runIfAllowed {
        database.fileInfo().findFileInfoBySearchTerm(query).map { it.native }
    }

    override fun findFileInfoLastModified(folder: String, path: String): Date? = runIfAllowed {
        database.fileInfo().findFileInfoLastModified(folder, path)?.lastModified
    }

    override fun findNotDeletedFileInfo(folder: String, path: String) = runIfAllowed {
        database.fileInfo().findNotDeletedFileInfo(folder, path)?.native
    }

    override fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String) = runIfAllowed {
        database.fileInfo().findNotDeletedFilesByFolderAndParent(folder, parentPath).map { it.native }
    }

    override fun countFileInfoBySearchTerm(query: String) = runIfAllowed {
        database.fileInfo().countFileInfoBySearchTerm(query)
    }

    override fun updateFileInfo(fileInfo: FileInfo, fileBlocks: FileBlocks?) = runIfAllowed {
        if (fileBlocks != null) {
            FileInfo.checkBlocks(fileInfo, fileBlocks)

            database.fileBlocks().mergeBlock(FileBlocksItem.fromNative(fileBlocks))
        }

        database.fileInfo().updateFileInfo(FileInfoItem.fromNative(fileInfo))
    }

    override fun updateFileInfoAndBlocks(fileInfos: List<FileInfo>, fileBlocks: List<FileBlocks>) = runIfAllowed {
        if (fileInfos.isNotEmpty()) {
            database.fileInfo().updateFileInfo(fileInfos.map { FileInfoItem.fromNative(it) })
        }

        if (fileBlocks.isNotEmpty()) {
            database.fileBlocks().mergeBlocks(fileBlocks.map { FileBlocksItem.fromNative(it) })
        }
    }

    // FileBlocks

    override fun findFileBlocks(folder: String, path: String) = runIfAllowed {
        database.fileBlocks().findFileBlocks(folder, path)?.native
    }

    // FolderStats

    override fun findAllFolderStats() = runIfAllowed {
        database.folderStats().findAllFolderStats().map { it.native }
    }

    override fun findFolderStats(folder: String): FolderStats? = runIfAllowed {
        database.folderStats().findFolderStats(folder)?.native
    }

    override fun updateOrInsertFolderStats(
            folder: String,
            deltaFileCount: Long,
            deltaDirCount: Long,
            deltaSize: Long,
            lastUpdate: Date
    ) = runIfAllowed {
        if (database.folderStats().updateFolderStats(folder, deltaFileCount, deltaDirCount, deltaSize, lastUpdate) == 0L) {
            database.folderStats().insertFolderStats(FolderStatsItem(folder, deltaFileCount, deltaDirCount, lastUpdate, deltaSize))
        }
    }

    // IndexInfo

    override fun updateIndexInfo(indexInfo: IndexInfo) = runIfAllowed {
        database.folderIndexInfo().updateIndexInfo(FolderIndexInfoItem.fromNative(indexInfo))
    }

    override fun findIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): IndexInfo? = runIfAllowed {
        database.folderIndexInfo().findIndexInfoByDeviceAndFolder(deviceId, folder)?.native
    }

    override fun findAllIndexInfos(): List<IndexInfo> = runIfAllowed {
        database.folderIndexInfo().findAllIndexInfo().map { it.native }
    }

    // managment

    override fun clearIndex() {
        runIfAllowed {
            database.clearAllTables()
            clearTempStorageHook()
        }
    }

    // other
    private val sequencer = object: Sequencer {
        private fun getDatabaseEntry(): IndexSequenceItem {
            val entry = database.indexSequence().getItem()

            if (entry != null) {
                return entry
            }

            val newEntry = IndexSequenceItem(
                    indexId = Math.abs(Random().nextLong()) + 1,
                    currentSequence = Math.abs(Random().nextLong()) + 1
            )

            database.indexSequence().createItem(newEntry)

            return newEntry
        }

        override fun indexId() = runIfAllowed { getDatabaseEntry().indexId }
        override fun currentSequence() = runIfAllowed { getDatabaseEntry().currentSequence }

        override fun nextSequence(): Long = runIfAllowed {
            database.indexSequence().incrementSequenceNumber(indexId())

            currentSequence()
        }
    }

    override fun getSequencer() = sequencer
}
