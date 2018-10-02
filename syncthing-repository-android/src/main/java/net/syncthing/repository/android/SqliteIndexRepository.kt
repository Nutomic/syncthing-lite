package net.syncthing.repository.android

import net.syncthing.java.core.beans.*
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.repository.android.database.RepositoryDatabase
import net.syncthing.repository.android.database.item.*
import java.util.*

class SqliteIndexRepository(
        private val database: RepositoryDatabase,
        private val closeDatabaseOnClose: Boolean,
        private val clearTempStorageHook: () -> Unit
): IndexRepository {
    private var folderStatsChangeListener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)? = null

    // FileInfo
    override fun findFileInfo(folder: String, path: String) = database.fileInfo().findFileInfo(folder, path)?.native
    override fun findFileInfoBySearchTerm(query: String) = database.fileInfo().findFileInfoBySearchTerm(query).map { it.native }
    override fun findFileInfoLastModified(folder: String, path: String): Date? = database.fileInfo().findFileInfoLastModified(folder, path)?.lastModified
    override fun findNotDeletedFileInfo(folder: String, path: String) = database.fileInfo().findNotDeletedFileInfo(folder, path)?.native
    override fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String) = database.fileInfo().findNotDeletedFilesByFolderAndParent(folder, parentPath).map { it.native }
    override fun countFileInfoBySearchTerm(query: String) = database.fileInfo().countFileInfoBySearchTerm(query)

    override fun updateFileInfo(fileInfo: FileInfo, fileBlocks: FileBlocks?) {
        val newFileInfo = fileInfo
        val newFileBlocks = fileBlocks

        database.runInTransaction {
            if (newFileBlocks != null) {
                FileInfo.checkBlocks(newFileInfo, newFileBlocks)

                database.fileBlocks().mergeBlock(FileBlocksItem.fromNative(newFileBlocks))
            }

            val oldFileInfo = findFileInfo(newFileInfo.folder, newFileInfo.path)

            database.fileInfo().updateFileInfo(FileInfoItem.fromNative(newFileInfo))

            //update stats
            var deltaFileCount = 0L
            var deltaDirCount= 0L
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

            folderStatsChangeListener?.invoke(object : IndexRepository.FolderStatsUpdatedEvent() {
                override fun getFolderStats(): List<FolderStats> {
                    return listOf(newFolderStats.native)
                }
            })
        }
    }

    // FileBlocks

    override fun findFileBlocks(folder: String, path: String) = database.fileBlocks().findFileBlocks(folder, path)?.native

    // FolderStats

    override fun findAllFolderStats() = database.folderStats().findAllFolderStats().map { it.native }

    override fun findFolderStats(folder: String): FolderStats? = database.folderStats().findFolderStats(folder)?.native

    // IndexInfo

    override fun updateIndexInfo(indexInfo: IndexInfo) {
        database.folderIndexInfo().updateIndexInfo(FolderIndexInfoItem.fromNative(indexInfo))
    }

    override fun findIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): IndexInfo? = database.folderIndexInfo().findIndexInfoByDeviceAndFolder(deviceId, folder)?.native

    // managment

    override fun clearIndex() {
        database.clearAllTables()
        clearTempStorageHook()
    }

    override fun close() {
        if (closeDatabaseOnClose) {
            database.close()
        }
    }

    // other
    private val sequencer = object: Sequencer {
        fun getDatabaseEntry(): IndexSequenceItem {
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

        override fun indexId() = getDatabaseEntry().indexId
        override fun currentSequence() = getDatabaseEntry().currentSequence

        override fun nextSequence(): Long {
            database.indexSequence().incrementSequenceNumber(indexId())

            return currentSequence()
        }
    }

    override fun getSequencer() = sequencer

    override fun setOnFolderStatsUpdatedListener(listener: ((IndexRepository.FolderStatsUpdatedEvent) -> Unit)?) {
        folderStatsChangeListener = listener
    }
}
