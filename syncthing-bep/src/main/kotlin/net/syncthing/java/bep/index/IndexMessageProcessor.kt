package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.interfaces.IndexRepository
import org.slf4j.LoggerFactory

object NewIndexMessageProcessor {
    private val logger = LoggerFactory.getLogger(NewIndexMessageProcessor::class.java)

    fun doHandleIndexMessageReceivedEvent(
            message: BlockExchangeProtos.IndexUpdate,
            peerDeviceId: DeviceId,
            indexRepository: IndexRepository,
            indexBrowsers: Set<IndexBrowser>,
            markActive: () -> Unit
    ): Pair<IndexInfo, List<FileInfo>> {
        val folderId = message.folder

        logger.debug("processing {} index records for folder {}", message.filesList.size, folderId)

        return indexRepository.runInTransaction { transaction ->
            val newRecords = mutableListOf<FileInfo>()
            var sequence: Long = -1

            for (fileInfo in message.filesList) {
                markActive()

                val newRecord = IndexElementProcessor.pushRecord(transaction, folderId, fileInfo, indexBrowsers)

                if (newRecord != null) {
                    newRecords.add(newRecord)
                }

                sequence = Math.max(fileInfo.sequence, sequence)
                markActive()
            }

            val newIndexInfo = UpdateIndexInfo.updateIndexInfo(transaction, folderId, peerDeviceId, null, null, sequence)

            newIndexInfo to newRecords.toList()
        }
    }
}
