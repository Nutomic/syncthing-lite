package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.IndexTransaction
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.util.*

object IndexElementProcessor {
    val logger = LoggerFactory.getLogger(IndexElementProcessor::class.java)

    fun pushRecord(
            transaction: IndexTransaction,
            folder: String,
            bepFileInfo: BlockExchangeProtos.FileInfo,
            folderStatsUpdateCollector: FolderStatsUpdateCollector,
            oldRecord: FileInfo?
    ): FileInfo? {
        var fileBlocks: FileBlocks? = null
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(bepFileInfo.name)
                .setLastModified(Date(bepFileInfo.modifiedS * 1000 + bepFileInfo.modifiedNs / 1000000))
                .setVersionList((if (bepFileInfo.hasVersion()) bepFileInfo.version.countersList else null ?: emptyList()).map { record -> FileInfo.Version(record.id, record.value) })
                .setDeleted(bepFileInfo.deleted)
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

        return addRecord(
                transaction = transaction,
                newRecord = builder.build(),
                fileBlocks = fileBlocks,
                folderStatsUpdateCollector = folderStatsUpdateCollector,
                oldRecord = oldRecord
        )
    }

    private fun addRecord(
            transaction: IndexTransaction,
            newRecord: FileInfo,
            oldRecord: FileInfo?,
            fileBlocks: FileBlocks?,
            folderStatsUpdateCollector: FolderStatsUpdateCollector
    ): FileInfo? {
        val lastModified = oldRecord?.lastModified

        return if (lastModified != null && newRecord.lastModified < lastModified) {
            logger.trace("discarding record = {}, modified before local record", newRecord)
            null
        } else {
            folderStatsUpdateCollector.put(transaction.updateFileInfo(newRecord, fileBlocks))
            logger.trace("loaded new record = {}", newRecord)

            newRecord
        }
    }
}
