/*
 * Copyright (C) 2016 Davide Imbriaco
 * Copyright (C) 2018 Jonas Lochmann
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
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.HashMap

class BlockPuller internal constructor(private val connectionHandler: ConnectionHandler,
                                       private val indexHandler: IndexHandler,
                                       private val responseHandler: ResponseHandler) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun pullFile(fileInfo: FileInfo): FileDownloadObserver {
        val fileBlocks = indexHandler.waitForRemoteIndexAcquired(connectionHandler)
                .getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                ?.value
                ?: throw IOException("file not found in local index for folder = ${fileInfo.folder} path = ${fileInfo.path}")
        logger.info("pulling file = {}", fileBlocks)
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(fileBlocks.folder), {"supplied connection handler $connectionHandler will not share folder ${fileBlocks.folder}"})

        val lock = Object()
        // TODO: things like sumBy work with integers, it would be better to use longs directly
        val totalTransferSize = fileBlocks.blocks.distinctBy { it.hash }.sumBy { it.size }

        // TODO: keeping this in memory can cause problems with big files
        val blocksByHash = Collections.synchronizedMap(HashMap<String, ByteArray>())

        val error = AtomicReference<Exception>()
        val fileDownloadObserver = object : FileDownloadObserver() {

            private fun receivedData() = blocksByHash.values.sumBy { it.size }.toLong()

            private fun totalData() = totalTransferSize.toLong()

            override fun progress() = if (isCompleted()) 1.0 else receivedData() / totalData().toDouble()

            override fun progressMessage() = (Math.round(progress() * 1000.0) / 10.0).toString() + "% " +
                    FileUtils.byteCountToDisplaySize(receivedData()) + " / " + FileUtils.byteCountToDisplaySize(totalData())

            override fun isCompleted() = fileBlocks.blocks.size == blocksByHash.size

            override fun inputStream(): InputStream {
                NetworkUtils.assertProtocol(isCompleted()) {"pull failed, some blocks are still missing"}
                val blockList = fileBlocks.blocks.map { blocksByHash[it.hash] }.toList()
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
                blocksByHash.clear()
            }
        }

        synchronized(lock) {
            for (block in fileBlocks.blocks.distinctBy { it.hash }) {
                val requestId = responseHandler.registerListener {
                    response ->

                    try {
                        synchronized(lock) {
                            NetworkUtils.assertProtocol(response.code == ErrorCode.NO_ERROR, { "received error response, code = ${response.code}" })
                            val data = response.data.toByteArray()
                            val hash = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))

                            if (hash != block.hash) {
                                throw IllegalStateException("expected block with hash ${block.hash}, but got block with hash $hash")
                            }

                            blocksByHash[hash] = data
                            logger.debug("aquired block, hash = {}", hash)
                            lock.notify()
                        }
                    } catch (ex: Exception) {
                        error.compareAndSet(null, ex)
                        lock.notify()
                    }
                }

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

            return fileDownloadObserver
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
