package net.syncthing.java.bep.index

import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.interfaces.IndexTransaction

object UpdateIndexInfo {
    fun updateIndexInfo(
            transaction: IndexTransaction,
            folder: String,
            deviceId: DeviceId,
            indexId: Long?,
            maxSequence: Long?,
            localSequence: Long?
    ): IndexInfo {
        val oldIndexSequenceInfo = transaction.findIndexInfoByDeviceAndFolder(deviceId, folder)

        var newIndexSequenceInfo = oldIndexSequenceInfo ?: kotlin.run {
            assert(indexId != null) {
                "index sequence info not found, and supplied null index id (folder = $folder, device = $deviceId)"
            }

            IndexInfo(
                    folderId = folder,
                    deviceId = deviceId.deviceId,
                    indexId = indexId!!,
                    localSequence = 0,
                    maxSequence = -1
            )
        }

        if (indexId != null && indexId != newIndexSequenceInfo.indexId) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(indexId = indexId)
        }

        if (maxSequence != null && maxSequence > newIndexSequenceInfo.maxSequence) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(maxSequence = maxSequence)
        }

        if (localSequence != null && localSequence > newIndexSequenceInfo.localSequence) {
            newIndexSequenceInfo = newIndexSequenceInfo.copy(localSequence = localSequence)
        }

        if (oldIndexSequenceInfo != newIndexSequenceInfo) {
            transaction.updateIndexInfo(newIndexSequenceInfo)
        }

        return newIndexSequenceInfo
    }
}
