/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.bep

import com.google.protobuf.ByteString
import net.syncthing.java.bep.BlockExchangeProtos.Vector
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.utils.BlockUtils
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.submitLogging
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.tuple.Pair
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class BlockPusher internal constructor(private val localDeviceId: DeviceId,
                                       private val connectionHandler: ConnectionHandler,
                                       private val indexHandler: IndexHandler) {

    private val logger = LoggerFactory.getLogger(javaClass)


    fun pushDelete(folderId: String, targetPath: String): IndexEditObserver {
        val fileInfo = indexHandler.waitForRemoteIndexAcquired(connectionHandler).getFileInfoByPath(folderId, targetPath)!!
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(fileInfo.folder), {"supplied connection handler $connectionHandler will not share folder ${fileInfo.folder}"})
        return IndexEditObserver(sendIndexUpdate(folderId, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(targetPath)
                .setType(BlockExchangeProtos.FileInfoType.valueOf(fileInfo.type.name))
                .setDeleted(true), fileInfo.versionList))
    }

    fun pushDir(folder: String, path: String): IndexEditObserver {
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(folder), {"supplied connection handler $connectionHandler will not share folder $folder"})
        return IndexEditObserver(sendIndexUpdate(folder, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(path)
                .setType(BlockExchangeProtos.FileInfoType.DIRECTORY), null))
    }

    fun pushFile(inputStream: InputStream, folderId: String, targetPath: String): FileUploadObserver {
        val fileInfo = indexHandler.waitForRemoteIndexAcquired(connectionHandler).getFileInfoByPath(folderId, targetPath)
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(folderId), {"supplied connection handler $connectionHandler will not share folder $folderId"})
        assert(fileInfo == null || fileInfo.folder == folderId)
        assert(fileInfo == null || fileInfo.path == targetPath)
        val monitoringProcessExecutorService = Executors.newCachedThreadPool()
        val dataSource = DataSource(inputStream)
        val fileSize = dataSource.size
        val sentBlocks = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
        val uploadError = AtomicReference<Exception>()
        val isCompleted = AtomicBoolean(false)
        val updateLock = Object()
        val listener = {request: BlockExchangeProtos.Request ->
            if (request.folder == folderId && request.name == targetPath) {
                val hash = Hex.toHexString(request.hash.toByteArray())
                logger.debug("handling block request = {}:{}-{} ({})", request.name, request.offset, request.size, hash)
                val data = dataSource.getBlock(request.offset, request.size, hash)
                val future = connectionHandler.sendMessage(BlockExchangeProtos.Response.newBuilder()
                        .setCode(BlockExchangeProtos.ErrorCode.NO_ERROR)
                        .setData(ByteString.copyFrom(data))
                        .setId(request.id)
                        .build())
                monitoringProcessExecutorService.submitLogging {
                    try {
                        future.get()
                        sentBlocks.add(hash)
                        synchronized(updateLock) {
                            updateLock.notifyAll()
                        }
                        //TODO retry on error, register error and throw on watcher
                    } catch (ex: InterruptedException) {
                        //return and do nothing
                    } catch (ex: ExecutionException) {
                        uploadError.set(ex)
                        synchronized(updateLock) {
                            updateLock.notifyAll()
                        }
                    }
                }
            }
        }
        connectionHandler.registerOnRequestMessageReceivedListeners(listener)
        logger.debug("send index update for file = {}", targetPath)
        val indexListener = { folderInfo: FolderInfo, newRecords: List<FileInfo>, _: IndexInfo ->
            if (folderInfo.folderId == folderId) {
                for (fileInfo2 in newRecords) {
                    if (fileInfo2.path == targetPath && fileInfo2.hash == dataSource.getHash()) { //TODO check not invalid
                        //                                sentBlocks.addAll(dataSource.getHashes());
                        isCompleted.set(true)
                        synchronized(updateLock) {
                            updateLock.notifyAll()
                        }
                    }
                }
            }
        }
        indexHandler.registerOnIndexRecordAcquiredListener(indexListener)
        val indexUpdate = sendIndexUpdate(folderId, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(targetPath)
                .setSize(fileSize)
                .setType(BlockExchangeProtos.FileInfoType.FILE)
                .addAllBlocks(dataSource.blocks), fileInfo?.versionList).right
        return object : FileUploadObserver() {

            override fun progressPercentage() = if (isCompleted.get()) 100 else (sentBlocks.size.toFloat() / dataSource.getHashes().size).toInt()

            // return sentBlocks.size() == dataSource.getHashes().size();
            override fun isCompleted() = isCompleted.get()

            override fun close() {
                logger.debug("closing upload process")
                monitoringProcessExecutorService.shutdown()
                indexHandler.unregisterOnIndexRecordAcquiredListener(indexListener)
                connectionHandler.unregisterOnRequestMessageReceivedListeners(listener)
                val fileInfo1 = indexHandler.pushRecord(indexUpdate.folder, indexUpdate.filesList.single())
                logger.info("sent file info record = {}", fileInfo1)
            }

            @Throws(InterruptedException::class, IOException::class)
            override fun waitForProgressUpdate(): Int {
                synchronized(updateLock) {
                    updateLock.wait()
                }
                if (uploadError.get() != null) {
                    throw IOException(uploadError.get())
                }
                return progressPercentage()
            }

        }
    }

    private fun sendIndexUpdate(folderId: String, fileInfoBuilder: BlockExchangeProtos.FileInfo.Builder,
                                oldVersions: Iterable<Version>?): Pair<Future<*>, BlockExchangeProtos.IndexUpdate> {
        run {
            val nextSequence = indexHandler.sequencer().nextSequence()
            val list = oldVersions ?: emptyList()
            logger.debug("version list = {}", list)
            val id = ByteBuffer.wrap(localDeviceId.toHashData()).long
            val version = BlockExchangeProtos.Counter.newBuilder()
                    .setId(id)
                    .setValue(nextSequence)
                    .build()
            logger.debug("append new version = {}", version)
            fileInfoBuilder
                    .setSequence(nextSequence)
                    .setVersion(Vector.newBuilder().addAllCounters(list.map { record ->
                        BlockExchangeProtos.Counter.newBuilder().setId(record.id).setValue(record.value).build()
                    })
                            .addCounters(version))
        }
        val lastModified = Date()
        val fileInfo = fileInfoBuilder
                .setModifiedS(lastModified.time / 1000)
                .setModifiedNs((lastModified.time % 1000 * 1000000).toInt())
                .setNoPermissions(true)
                .build()
        val indexUpdate = BlockExchangeProtos.IndexUpdate.newBuilder()
                .setFolder(folderId)
                .addFiles(fileInfo)
                .build()
        logger.debug("index update = {}", fileInfo)
        return Pair.of(connectionHandler.sendMessage(indexUpdate), indexUpdate)
    }

    abstract inner class FileUploadObserver : Closeable {

        abstract fun progressPercentage(): Int

        abstract fun isCompleted(): Boolean

        @Throws(InterruptedException::class)
        abstract fun waitForProgressUpdate(): Int

        @Throws(InterruptedException::class)
        fun waitForComplete(): FileUploadObserver {
            while (!isCompleted()) {
                waitForProgressUpdate()
            }
            return this
        }
    }

    inner class IndexEditObserver(private val future: Future<*>, private val indexUpdate: BlockExchangeProtos.IndexUpdate) : Closeable {

        //throw exception if job has errors
        @Throws(InterruptedException::class, ExecutionException::class)
        fun isCompleted(): Boolean {
            return if (future.isDone) {
                future.get()
                true
            } else {
                false
            }
        }

        constructor(pair: Pair<Future<*>, BlockExchangeProtos.IndexUpdate>) : this(pair.left, pair.right)

        @Throws(InterruptedException::class, ExecutionException::class)
        fun waitForComplete() {
            future.get()
        }

        @Throws(IOException::class)
        override fun close() {
            indexHandler.pushRecord(indexUpdate.folder, indexUpdate.filesList.single())
        }

    }

    private class DataSource @Throws(IOException::class) constructor(private val inputStream: InputStream) {

        var size: Long = 0
            private set
        lateinit var blocks: List<BlockExchangeProtos.BlockInfo>
            private set
        private var hashes: Set<String>? = null

        private var hash: String? = null

        init {
            inputStream.use { it ->
                val list = mutableListOf<BlockExchangeProtos.BlockInfo>()
                var offset: Long = 0
                while (true) {
                    var block = ByteArray(BLOCK_SIZE)
                    val blockSize = it.read(block)
                    if (blockSize <= 0) {
                        break
                    }
                    if (blockSize < block.size) {
                        block = Arrays.copyOf(block, blockSize)
                    }

                    val hash = MessageDigest.getInstance("SHA-256").digest(block)
                    list.add(BlockExchangeProtos.BlockInfo.newBuilder()
                            .setHash(ByteString.copyFrom(hash))
                            .setOffset(offset)
                            .setSize(blockSize)
                            .build())
                    offset += blockSize.toLong()
                }
                size = offset
                blocks = list
            }
        }

        @Throws(IOException::class)
        fun getBlock(offset: Long, size: Int, hash: String): ByteArray {
            val buffer = ByteArray(size)
            inputStream.use { it ->
                IOUtils.skipFully(it, offset)
                IOUtils.readFully(it, buffer)
                NetworkUtils.assertProtocol(Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(buffer)) == hash, {"block hash mismatch!"})
                return buffer
            }
        }


        fun getHashes(): Set<String> {
            return hashes ?: let {
                val hashes2 = blocks.map { input -> Hex.toHexString(input.hash.toByteArray()) }.toSet()
                hashes = hashes2
                return hashes2
            }
        }

        fun getHash(): String {
            return hash ?: let {
                val blockInfo = blocks.map { input ->
                    BlockInfo(input.offset, input.size, Hex.toHexString(input.hash.toByteArray()))
                }
                val hash2 = BlockUtils.hashBlocks(blockInfo)
                hash = hash2
                hash2
            }
        }
    }

    companion object {

        const val BLOCK_SIZE = 128 * 1024
    }

}
