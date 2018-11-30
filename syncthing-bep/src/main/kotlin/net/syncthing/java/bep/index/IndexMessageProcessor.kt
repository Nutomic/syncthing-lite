package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.interfaces.IndexTransaction
import org.slf4j.LoggerFactory

object IndexMessageProcessor {
    private val logger = LoggerFactory.getLogger(IndexMessageProcessor::class.java)

    fun doHandleIndexMessageReceivedEvent(
            message: BlockExchangeProtos.IndexUpdate,
            peerDeviceId: DeviceId,
            transaction: IndexTransaction
    ): Result {
        val folderId = message.folder

        logger.debug("processing {} index records for folder {}", message.filesList.size, folderId)

        val newRecords = mutableListOf<FileInfo>()
        var sequence: Long = -1

        // this always keeps the last version per path
        val filesToProcess = message.filesList
                .sortedBy { it.sequence }
                .reversed()
                .distinctBy { it.name /* this is the whole path */ }
                .reversed()
                .toList()

        val relatedFileInfo = transaction.findFileInfo(folderId, filesToProcess.map { it.name })
        val folderStatsUpdateCollector = FolderStatsUpdateCollector(message.folder)

        for (fileInfo in filesToProcess) {
            val newRecord = IndexElementProcessor.pushRecord(
                    transaction = transaction,
                    folder = folderId,
                    bepFileInfo = fileInfo,
                    folderStatsUpdateCollector = folderStatsUpdateCollector,
                    oldRecord = relatedFileInfo[fileInfo.name]
            )

            if (newRecord != null) {
                newRecords.add(newRecord)
            }

            sequence = Math.max(fileInfo.sequence, sequence)
        }

        handleFolderStatsUpdate(transaction, folderStatsUpdateCollector)

        val newIndexInfo = UpdateIndexInfo.updateIndexInfo(transaction, folderId, peerDeviceId, null, null, sequence)

        return Result(newIndexInfo, newRecords.toList(), transaction.findFolderStats(folderId) ?: FolderStats.createDummy(folderId))
    }

    fun handleFolderStatsUpdate(transaction: IndexTransaction, folderStatsUpdateCollector: FolderStatsUpdateCollector) {
        if (folderStatsUpdateCollector.isEmpty()) {
            return
        }

        transaction.updateOrInsertFolderStats(
                folder = folderStatsUpdateCollector.folderId,
                deltaSize = folderStatsUpdateCollector.deltaSize,
                deltaFileCount = folderStatsUpdateCollector.deltaFileCount,
                deltaDirCount = folderStatsUpdateCollector.deltaDirCount,
                lastUpdate = folderStatsUpdateCollector.lastModified
        )
    }

    data class Result(val newIndexInfo: IndexInfo, val updatedFiles: List<FileInfo>, val newFolderStats: FolderStats)
}
