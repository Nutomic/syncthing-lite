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
import net.syncthing.java.bep.BlockExchangeProtos.ErrorCode
import net.syncthing.java.bep.BlockExchangeProtos.Request
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.utils.NetworkUtils
import org.apache.commons.io.FileUtils
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.*
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class BlockPuller internal constructor(private val connectionHandler: ConnectionHandler,
                                       private val indexHandler: IndexHandler) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val blocksByHash = ConcurrentHashMap<String, ByteArray>()
    private val hashList = mutableListOf<String>()
    private val missingHashes: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
    private val requestIds: MutableSet<Int> = Collections.newSetFromMap(ConcurrentHashMap<Int, Boolean>())
    private val lock = Object()

    fun pullFile(fileInfo: FileInfo): FileDownloadObserver {
        val fileBlocks = indexHandler.waitForRemoteIndexAcquired(connectionHandler)
                .getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                ?.value
                ?: throw IOException("file not found in local index for folder = ${fileInfo.folder} path = ${fileInfo.path}")
        logger.info("pulling file = {}", fileBlocks)
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(fileBlocks.folder), {"supplied connection handler $connectionHandler will not share folder ${fileBlocks.folder}"})
        val error = AtomicReference<Exception>()
        val fileDownloadObserver = object : FileDownloadObserver() {

            private fun receivedData() = (blocksByHash.size * BlockPusher.BLOCK_SIZE).toLong()

            private fun totalData() = ((blocksByHash.size + missingHashes.size) * BlockPusher.BLOCK_SIZE).toLong()

            override fun progress() = if (isCompleted()) 1.0 else receivedData() / totalData().toDouble()

            override fun progressMessage() = (Math.round(progress() * 1000.0) / 10.0).toString() + "% " +
                    FileUtils.byteCountToDisplaySize(receivedData()) + " / " + FileUtils.byteCountToDisplaySize(totalData())

            override fun isCompleted() = missingHashes.isEmpty()

            override fun inputStream(): InputStream {
                    NetworkUtils.assertProtocol(missingHashes.isEmpty(), {"pull failed, some blocks are still missing"})
                    val blockList = hashList.map { blocksByHash[it] }.toList()
                    return SequenceInputStream(Collections.enumeration(blockList.map { ByteArrayInputStream(it) }))
                }

            override fun checkError() {
                if (error.get() != null) {
                    throw IOException(error.get())
                }
            }

            @Throws(InterruptedException::class)
            override fun waitForProgressUpdate(): Double {
                if (!isCompleted()) {
                    synchronized(lock) {
                        checkError()
                        lock.wait()
                        checkError()
                    }
                }
                return progress()
            }

            override fun close() {
                missingHashes.clear()
                hashList.clear()
                blocksByHash.clear()
            }
        }
        synchronized(lock) {
            hashList.addAll(fileBlocks.blocks.map { it.hash })
            missingHashes.addAll(hashList)
            for (block in fileBlocks.blocks) {
                if (missingHashes.contains(block.hash)) {
                    val requestId = Math.abs(Random().nextInt())
                    requestIds.add(requestId)
                    connectionHandler.sendMessage(Request.newBuilder()
                            .setId(requestId)
                            .setFolder(fileBlocks.folder)
                            .setName(fileBlocks.path)
                            .setOffset(block.offset)
                            .setSize(block.size)
                            .setHash(ByteString.copyFrom(Hex.decode(block.hash)))
                            .build())
                    logger.debug("sent request for block, hash = {}", block.hash)
                }
            }
            return fileDownloadObserver
        }
    }

    fun onResponseMessageReceived(response: BlockExchangeProtos.Response) {
        synchronized(lock) {
            if (!requestIds.contains(response.id)) {
                return
            }
            NetworkUtils.assertProtocol(response.code == ErrorCode.NO_ERROR, {"received error response, code = ${response.code}"})
            val data = response.data.toByteArray()
            val hash = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))
            if (missingHashes.remove(hash)) {
                blocksByHash.put(hash, data)
                logger.debug("aquired block, hash = {}", hash)
                lock.notify()
            } else {
                logger.warn("received not-needed block, hash = {}", hash)
            }
        }
    }

    abstract inner class FileDownloadObserver : Closeable {

        abstract fun progress(): Double

        abstract fun progressMessage(): String

        abstract fun isCompleted(): Boolean

        abstract fun inputStream(): InputStream

        abstract fun checkError()

        @Throws(InterruptedException::class)
        abstract fun waitForProgressUpdate(): Double

        @Throws(InterruptedException::class)
        fun waitForComplete(): FileDownloadObserver {
            while (!isCompleted()) {
                waitForProgressUpdate()
            }
            return this
        }

        abstract override fun close()

    }

}
