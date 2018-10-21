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
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import net.syncthing.java.bep.BlockExchangeProtos.ErrorCode
import net.syncthing.java.bep.BlockExchangeProtos.Request
import net.syncthing.java.bep.utils.longSumBy
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.utils.NetworkUtils
import org.apache.commons.io.FileUtils
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.*
import java.lang.Exception
import java.security.MessageDigest
import java.util.*
import kotlin.collections.HashMap

class BlockPuller internal constructor(private val connectionHandler: ConnectionHandler,
                                       private val indexHandler: IndexHandler,
                                       private val responseHandler: ResponseHandler,
                                       private val tempRepository: TempRepository) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun pullFileSync(
            fileInfo: FileInfo,
            progressListener: (status: BlockPullerStatus) -> Unit = {  }
    ): InputStream {
        return runBlocking {
            pullFileCoroutine(fileInfo, progressListener)
        }
    }

    suspend fun pullFileCoroutine(
            fileInfo: FileInfo,
            progressListener: (status: BlockPullerStatus) -> Unit = {  }
    ): InputStream {
        val fileBlocks = indexHandler.waitForRemoteIndexAcquired(connectionHandler)
                .getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                ?.value
                ?: throw IOException("file not found in local index for folder = ${fileInfo.folder} path = ${fileInfo.path}")
        logger.info("pulling file = {}", fileBlocks)
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(fileBlocks.folder), { "supplied connection handler $connectionHandler will not share folder ${fileBlocks.folder}" })

        // the file could have changed since the caller read it
        // this would save the file using a wrong name, so throw here
        if (fileBlocks.hash != fileInfo.hash) {
            throw IllegalStateException("the current file entry hash does not match the hash of the provided one")
        }

        val blockTempIdByHash = Collections.synchronizedMap(HashMap<String, String>())

        var status = BlockPullerStatus(
                downloadedBytes = 0,
                totalTransferSize = fileBlocks.blocks.distinctBy { it.hash }.longSumBy { it.size.toLong() },
                totalFileSize = fileBlocks.size
        )

        try {
            val reportProgressLock = Object()

            fun updateProgress(additionalDownloadedBytes: Long) {
                synchronized(reportProgressLock) {
                    status = status.copy(
                            downloadedBytes = status.downloadedBytes + additionalDownloadedBytes
                    )

                    progressListener(status)
                }
            }

            coroutineScope {
                val pipe = Channel<BlockInfo>()

                repeat(4 /* 4 blocks per time */) { workerNumber ->
                    async {
                        for (block in pipe) {
                            logger.debug("request block with hash = {} from worker {}", block.hash, workerNumber)

                            val blockContent = pullBlock(fileBlocks, block, 1000 * 60 /* 60 seconds timeout per block */)

                            blockTempIdByHash[block.hash] = tempRepository.pushTempData(blockContent)

                            updateProgress(blockContent.size.toLong())
                        }
                    }
                }

                fileBlocks.blocks.distinctBy { it.hash }.forEach { block ->
                    pipe.send(block)
                }

                pipe.close()
            }

            // the sequence is evaluated lazy -> only one block per time is loaded
            val fileBlocksIterator = fileBlocks.blocks
                    .asSequence()
                    .map { tempRepository.popTempData(blockTempIdByHash[it.hash]!!) }
                    .map { ByteArrayInputStream(it) }
                    .iterator()

            return object : SequenceInputStream(object : Enumeration<InputStream> {
                override fun hasMoreElements() = fileBlocksIterator.hasNext()
                override fun nextElement() = fileBlocksIterator.next()
            }) {
                override fun close() {
                    super.close()

                    // delete all temp blocks now
                    // they are deleted after reading, but the consumer could stop before reading the whole stream
                    tempRepository.deleteTempData(blockTempIdByHash.values.toList())
                }
            }
        } catch (ex: Exception) {
            // delete all temp blocks now
            tempRepository.deleteTempData(blockTempIdByHash.values.toList())

            throw ex
        }
    }

    private suspend fun pullBlock(fileBlocks: FileBlocks, block: BlockInfo, timeoutInMillis: Long): ByteArray {
        logger.debug("sent request for block, hash = {}", block.hash)

        val response =
                withTimeout(timeoutInMillis) {
                    try {
                        doRequest(
                                Request.newBuilder()
                                        .setFolder(fileBlocks.folder)
                                        .setName(fileBlocks.path)
                                        .setOffset(block.offset)
                                        .setSize(block.size)
                                        .setHash(ByteString.copyFrom(Hex.decode(block.hash)))
                        )
                    } catch (ex: TimeoutCancellationException) {
                        // It seems like the TimeoutCancellationException
                        // is handled differently so that the timeout is ignored.
                        // Due to that, it's converted to an IOException.

                        throw IOException("timeout during requesting block")
                    }
                }

        NetworkUtils.assertProtocol(response.code == ErrorCode.NO_ERROR) {
            "received error response, code = ${response.code}"
        }

        val data = response.data.toByteArray()
        val hash = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))

        if (hash != block.hash) {
            throw IllegalStateException("expected block with hash ${block.hash}, but got block with hash $hash")
        }

        return data
    }

    private suspend fun doRequest(request: Request.Builder): BlockExchangeProtos.Response {
        return suspendCancellableCoroutine { continuation ->
            val requestId = responseHandler.registerListener { response ->
                continuation.resume(response)
            }

            connectionHandler.sendMessage(
                    request
                            .setId(requestId)
                            .build()
            )
        }
    }
}

data class BlockPullerStatus(
        val downloadedBytes: Long,
        val totalTransferSize: Long,
        val totalFileSize: Long
)
