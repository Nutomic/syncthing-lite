package net.syncthing.java.bep.index

import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.interfaces.IndexTransaction

object UpdateIndexInfo {
    fun updateIndexInfoFromClusterConfig(
            transaction: IndexTransaction,
            folder: String,
            deviceId: DeviceId,
            indexId: Long,
            maxSequence: Long
    ): IndexInfo {
        val oldIndexSequenceInfo = transaction.findIndexInfoByDeviceAndFolder(deviceId, folder)

        var newIndexSequenceInfo = oldIndexSequenceInfo ?: IndexInfo(
                folderId = folder,
                deviceId = deviceId.deviceId,
                indexId = indexId,
                localSequence = 0,
                maxSequence = -1
        )

        if (indexId != newIndexSequenceInfo.indexId) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(indexId = indexId)
        }

        if (maxSequence > newIndexSequenceInfo.maxSequence) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(maxSequence = maxSequence)
        }

        if (oldIndexSequenceInfo != newIndexSequenceInfo) {
            transaction.updateIndexInfo(newIndexSequenceInfo)
        }

        return newIndexSequenceInfo
    }

    fun updateIndexInfoFromIndexElementProcessor(
            transaction: IndexTransaction,
            oldIndexInfo: IndexInfo,
            localSequence: Long?
    ): IndexInfo {
        var newIndexSequenceInfo = oldIndexInfo

        if (localSequence != null && localSequence > newIndexSequenceInfo.localSequence) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(localSequence = localSequence)
        }

        if (oldIndexInfo != newIndexSequenceInfo) {
            transaction.updateIndexInfo(newIndexSequenceInfo)
        }

        return newIndexSequenceInfo
    }
}
