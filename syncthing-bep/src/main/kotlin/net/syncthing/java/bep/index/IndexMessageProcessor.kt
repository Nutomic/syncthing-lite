package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.interfaces.IndexTransaction
import org.slf4j.LoggerFactory

object NewIndexMessageProcessor {
    private val logger = LoggerFactory.getLogger(NewIndexMessageProcessor::class.java)

    fun doHandleIndexMessageReceivedEvent(
            message: BlockExchangeProtos.IndexUpdate,
            peerDeviceId: DeviceId,
            transaction: IndexTransaction
    ): Pair<IndexInfo, List<FileInfo>> {
        val folderId = message.folder

        logger.debug("processing {} index records for folder {}", message.filesList.size, folderId)

        val newRecords = mutableListOf<FileInfo>()
        var sequence: Long = -1

        for (fileInfo in message.filesList) {
            val newRecord = IndexElementProcessor.pushRecord(transaction, folderId, fileInfo)

            if (newRecord != null) {
                newRecords.add(newRecord)
            }

            sequence = Math.max(fileInfo.sequence, sequence)
        }

        val newIndexInfo = UpdateIndexInfo.updateIndexInfo(transaction, folderId, peerDeviceId, null, null, sequence)

        return newIndexInfo to newRecords.toList()
    }
}
