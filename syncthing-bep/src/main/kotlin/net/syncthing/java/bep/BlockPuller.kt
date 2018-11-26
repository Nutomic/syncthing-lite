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
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.bep.utils.longSumBy
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.TempRepository
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.*
import java.security.MessageDigest
import java.util.*
import kotlin.collections.HashMap

object BlockPuller {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun pullFile(
            fileInfo: FileInfo,
            progressListener: (status: BlockPullerStatus) -> Unit = {  },
            connections: List<ConnectionActorWrapper>,
            indexHandler: IndexHandler,
            tempRepository: TempRepository
    ): InputStream {
        val connectionHelper = MultiConnectionHelper(connections) {
            it.hasFolder(fileInfo.folder)
        }

        // fail early if there is no matching connection
        connectionHelper.pickConnection()

        val (newFileInfo, fileBlocks) = indexHandler.getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path) ?: throw FileNotFoundException()

        if (fileBlocks.hash != fileInfo.hash) {
            throw IllegalStateException("the current file entry hash does not match the hash of the provided one")
        }

        logger.info("pulling file = {}", fileBlocks)

        val blockTempIdByHash = Collections.synchronizedMap(HashMap<String, String>())

        var status = BlockPullerStatus(
                downloadedBytes = 0,
                totalTransferSize = fileBlocks.blocks.distinctBy { it.hash }.longSumBy { it.size.toLong() },
                totalFileSize = fileBlocks.size
        )

        suspend fun pullBlock(fileBlocks: FileBlocks, block: BlockInfo, timeoutInMillis: Long, connectionActorWrapper: ConnectionActorWrapper): ByteArray {
            logger.debug("sent message for block, hash = {}", block.hash)

            val response =
                    withTimeout(timeoutInMillis) {
                        try {
                            connectionActorWrapper.sendRequest(
                                    BlockExchangeProtos.Request.newBuilder()
                                            .setFolder(fileBlocks.folder)
                                            .setName(fileBlocks.path)
                                            .setOffset(block.offset)
                                            .setSize(block.size)
                                            .setHash(ByteString.copyFrom(Hex.decode(block.hash)))
                                            .buildPartial()
                            )
                        } catch (ex: TimeoutCancellationException) {
                            // It seems like the TimeoutCancellationException
                            // is handled differently so that the timeout is ignored.
                            // Due to that, it's converted to an IOException.

                            throw IOException("timeout during requesting block")
                        }
                    }

            if (response.code != BlockExchangeProtos.ErrorCode.NO_ERROR) {
                // the server does not have/ want to provide this file -> don't ask him again
                connectionHelper.disableConnection(connectionActorWrapper)

                throw IOException("received error response ${response.code}")
            }

            val data = response.data.toByteArray()
            val hash = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))

            if (hash != block.hash) {
                throw IllegalStateException("expected block with hash ${block.hash}, but got block with hash $hash")
            }

            return data
        }

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
                            logger.debug("message block with hash = {} from worker {}", block.hash, workerNumber)

                            lateinit var blockContent: ByteArray

                            val attempts = 0..4

                            for (attempt in attempts) {
                                try {
                                    blockContent = pullBlock(fileBlocks, block, 1000 * 60 /* 60 seconds timeout per block */, connectionHelper.pickConnection())

                                    break
                                } catch (ex: IOException) {
                                    if (attempt == attempts.last) {
                                        throw ex
                                    } else {
                                        // will retry after a pause
                                        // 0: 300 ms after the first attempt
                                        // 1: 1200 ms after the second attempt
                                        // 2: 2700 ms after the third attempt
                                        // 3: 4800 ms after the third attempt
                                        // total: 9000 ms
                                        delay((attempt + 1) * (attempt + 1) * 300L)
                                    }
                                }
                            }

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
}

data class BlockPullerStatus(
        val downloadedBytes: Long,
        val totalTransferSize: Long,
        val totalFileSize: Long
)
