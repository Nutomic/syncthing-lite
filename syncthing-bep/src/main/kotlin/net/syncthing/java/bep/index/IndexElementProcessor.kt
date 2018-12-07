package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.exception.ExceptionDetailException
import net.syncthing.java.core.exception.ExceptionDetails
import net.syncthing.java.core.interfaces.IndexTransaction
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*

object IndexElementProcessor {
    private val logger = LoggerFactory.getLogger(IndexElementProcessor::class.java)

    fun pushRecords(
            transaction: IndexTransaction,
            folder: String,
            updates: List<BlockExchangeProtos.FileInfo>,
            oldRecords: Map<String, FileInfo>,
            folderStatsUpdateCollector: FolderStatsUpdateCollector
    ): List<FileInfo> {
        // this always keeps the last version per path
        val filesToProcess = updates
                .sortedBy { it.sequence }
                .reversed()
                .distinctBy { it.name /* this is the whole path */ }
                .reversed()

        val preparedUpdates = filesToProcess.mapNotNull { prepareUpdate(folder, it) }

        val updatesToApply = preparedUpdates.filter { shouldUpdateRecord(oldRecords[it.first.path], it.first) }

        transaction.updateFileInfoAndBlocks(
                fileInfos = updatesToApply.map { it.first },
                fileBlocks = updatesToApply.mapNotNull { it.second }
        )

        for ((newRecord) in updatesToApply) {
            updateFolderStatsCollector(oldRecords[newRecord.path], newRecord, folderStatsUpdateCollector)
        }

        return updatesToApply.map { it.first }
    }

    fun pushRecord(
            transaction: IndexTransaction,
            folder: String,
            bepFileInfo: BlockExchangeProtos.FileInfo,
            folderStatsUpdateCollector: FolderStatsUpdateCollector,
            oldRecord: FileInfo?
    ): FileInfo? {
        val update = prepareUpdate(folder, bepFileInfo)

        return if (update != null) {
            addRecord(
                    transaction = transaction,
                    newRecord = update.first,
                    fileBlocks = update.second,
                    folderStatsUpdateCollector = folderStatsUpdateCollector,
                    oldRecord = oldRecord
            )
        } else {
            null
        }
    }

    private fun prepareUpdate(
            folder: String,
            bepFileInfo: BlockExchangeProtos.FileInfo
    ): Pair<FileInfo, FileBlocks?>? {
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(bepFileInfo.name)
                .setLastModified(Date(bepFileInfo.modifiedS * 1000 + bepFileInfo.modifiedNs / 1000000))
                .setVersionList((if (bepFileInfo.hasVersion()) bepFileInfo.version.countersList else null ?: emptyList()).map { record -> FileInfo.Version(record.id, record.value) })
                .setDeleted(bepFileInfo.deleted)

        var fileBlocks: FileBlocks? = null

        when (bepFileInfo.type) {
            BlockExchangeProtos.FileInfoType.FILE -> {
                fileBlocks = FileBlocks(folder, builder.getPath()!!, ((bepFileInfo.blocksList ?: emptyList())).map { record ->
                    BlockInfo(record.offset, record.size, Hex.toHexString(record.hash.toByteArray()))
                })
                builder
                        .setTypeFile()
                        .setHash(fileBlocks.hash)
                        .setSize(bepFileInfo.size)
            }
            BlockExchangeProtos.FileInfoType.DIRECTORY -> builder.setTypeDir()
            else -> {
                logger.warn("unsupported file type = {}, discarding file info", bepFileInfo.type)
                return null
            }
        }

        return builder.build() to fileBlocks
    }

    private fun shouldUpdateRecord(
            oldRecord: FileInfo?,
            newRecord: FileInfo
    ) = oldRecord == null || newRecord.lastModified >= oldRecord.lastModified

    private fun addRecord(
            transaction: IndexTransaction,
            newRecord: FileInfo,
            oldRecord: FileInfo?,
            fileBlocks: FileBlocks?,
            folderStatsUpdateCollector: FolderStatsUpdateCollector
    ): FileInfo? {
        return if (shouldUpdateRecord(oldRecord, newRecord)) {
            logger.trace("discarding record = {}, modified before local record", newRecord)
            null
        } else {
            logger.trace("loaded new record = {}", newRecord)

            transaction.updateFileInfo(newRecord, fileBlocks)
            updateFolderStatsCollector(oldRecord, newRecord, folderStatsUpdateCollector)

            newRecord
        }
    }

    private fun updateFolderStatsCollector(
            oldRecord: FileInfo?,
            newRecord: FileInfo,
            folderStatsUpdateCollector: FolderStatsUpdateCollector
    ) {
        val oldMissing = oldRecord == null || oldRecord.isDeleted
        val newMissing = newRecord.isDeleted
        val oldSizeMissing = oldMissing || !oldRecord!!.isFile()
        val newSizeMissing = newMissing || !newRecord.isFile()

        if (!oldSizeMissing) {
            folderStatsUpdateCollector.deltaSize -= oldRecord!!.size!!
        }

        if (!newSizeMissing) {
            folderStatsUpdateCollector.deltaSize += newRecord.size!!
        }

        if (!oldMissing) {
            if (oldRecord!!.isFile()) {
                folderStatsUpdateCollector.deltaFileCount--
            } else if (oldRecord.isDirectory()) {
                folderStatsUpdateCollector.deltaDirCount--
            }
        }

        if (!newMissing) {
            if (newRecord.isFile()) {
                folderStatsUpdateCollector.deltaFileCount++
            } else if (newRecord.isDirectory()) {
                folderStatsUpdateCollector.deltaDirCount++
            }
        }

        folderStatsUpdateCollector.lastModified = newRecord.lastModified
    }
}
