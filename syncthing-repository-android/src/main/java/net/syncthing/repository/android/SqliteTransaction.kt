package net.syncthing.repository.android

import net.syncthing.java.core.beans.*
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.repository.android.database.RepositoryDatabase
import net.syncthing.repository.android.database.item.*
import java.util.*
import java.util.concurrent.Callable

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

    override fun updateFileInfo(fileInfo: FileInfo, fileBlocks: FileBlocks?): FolderStats = runIfAllowed {
        val newFileInfo = fileInfo
        val newFileBlocks = fileBlocks

        database.runInTransaction(object: Callable<FolderStats> {
            override fun call(): FolderStats {
                if (newFileBlocks != null) {
                    FileInfo.checkBlocks(newFileInfo, newFileBlocks)

                    database.fileBlocks().mergeBlock(FileBlocksItem.fromNative(newFileBlocks))
                }

                val oldFileInfo = findFileInfo(newFileInfo.folder, newFileInfo.path)

                database.fileInfo().updateFileInfo(FileInfoItem.fromNative(newFileInfo))

                //update stats
                var deltaFileCount = 0L
                var deltaDirCount = 0L
                var deltaSize = 0L
                val oldMissing = oldFileInfo == null || oldFileInfo.isDeleted
                val newMissing = newFileInfo.isDeleted
                val oldSizeMissing = oldMissing || !oldFileInfo!!.isFile()
                val newSizeMissing = newMissing || !newFileInfo.isFile()
                if (!oldSizeMissing) {
                    deltaSize -= oldFileInfo!!.size!!
                }
                if (!newSizeMissing) {
                    deltaSize += newFileInfo.size!!
                }
                if (!oldMissing) {
                    if (oldFileInfo!!.isFile()) {
                        deltaFileCount--
                    } else if (oldFileInfo.isDirectory()) {
                        deltaDirCount--
                    }
                }
                if (!newMissing) {
                    if (newFileInfo.isFile()) {
                        deltaFileCount++
                    } else if (newFileInfo.isDirectory()) {
                        deltaDirCount++
                    }
                }

                val newFolderStats = kotlin.run {
                    val updatedRows = database.folderStats().updateFolderStats(
                            folder = newFileInfo.folder,
                            deltaDirCount = deltaDirCount,
                            deltaFileCount = deltaFileCount,
                            deltaSize = deltaSize,
                            lastUpdate = newFileInfo.lastModified
                    )

                    if (updatedRows == 0L) {
                        database.folderStats().insertFolderStats(FolderStatsItem(
                                folder = newFileInfo.folder,
                                dirCount = deltaDirCount,
                                fileCount = deltaFileCount,
                                size = deltaSize,
                                lastUpdate = newFileInfo.lastModified
                        ))
                    }

                    database.folderStats().getFolderStats(newFileInfo.folder)!!
                }

                return newFolderStats.native
            }
        })
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
