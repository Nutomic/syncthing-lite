package net.syncthing.java.bep.index

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.IndexTransaction
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.util.*

object IndexElementProcessor {
    val logger = LoggerFactory.getLogger(IndexElementProcessor::class.java)

    fun pushRecord(transaction: IndexTransaction, folder: String, bepFileInfo: BlockExchangeProtos.FileInfo, indexBrowsers: Set<IndexBrowser>): FileInfo? {
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

        return addRecord(transaction, builder.build(), fileBlocks, indexBrowsers)
    }

    fun addRecord(transaction: IndexTransaction, record: FileInfo, fileBlocks: FileBlocks?, indexBrowsers: Set<IndexBrowser>): FileInfo? {
        val lastModified = transaction.findFileInfoLastModified(record.folder, record.path)
        return if (lastModified != null && record.lastModified < lastModified) {
            logger.trace("discarding record = {}, modified before local record", record)
            null
        } else {
            transaction.updateFileInfo(record, fileBlocks)
            logger.trace("loaded new record = {}", record)
            indexBrowsers.forEach {
                it.onIndexChangedevent(record.folder)
            }
            record
        }
    }
}
