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
import kotlinx.coroutines.experimental.channels.ClosedReceiveChannelException
import net.syncthing.java.bep.BlockExchangeProtos.ErrorCode
import net.syncthing.java.bep.BlockExchangeProtos.Request
import net.syncthing.java.bep.utils.longSumBy
import net.syncthing.java.core.beans.BlockInfo
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
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
                                       private val responseHandler: ResponseHandler) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun pullFileSync(
            fileInfo: FileInfo,
            progressListener: (progress: Double, progressMessage: String) -> Unit = { _, _ -> }
    ): InputStream {
        return runBlocking {
            pullFileCoroutine(fileInfo, progressListener)
        }
    }

    suspend fun pullFileCoroutine(
            fileInfo: FileInfo,
            progressListener: (progress: Double, progressMessage: String) -> Unit = { _, _ -> }
    ): InputStream {
        val fileBlocks = indexHandler.waitForRemoteIndexAcquired(connectionHandler)
                .getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                ?.value
                ?: throw IOException("file not found in local index for folder = ${fileInfo.folder} path = ${fileInfo.path}")
        logger.info("pulling file = {}", fileBlocks)
        NetworkUtils.assertProtocol(connectionHandler.hasFolder(fileBlocks.folder), { "supplied connection handler $connectionHandler will not share folder ${fileBlocks.folder}" })

        val totalTransferSize = fileBlocks.blocks.distinctBy { it.hash }.longSumBy { it.size.toLong() }

        // TODO: keeping this in memory can cause problems with big files
        val blocksByHash = Collections.synchronizedMap(HashMap<String, ByteArray>())

        fun updateProgress() {
            synchronized(blocksByHash) {
                val receivedData = blocksByHash.values.longSumBy { it.size.toLong() }
                val progress = totalTransferSize / receivedData.toDouble()
                val progressMessage = (Math.round(progress * 1000.0) / 10.0).toString() + "% " +
                        FileUtils.byteCountToDisplaySize(receivedData) + " / " + FileUtils.byteCountToDisplaySize(totalTransferSize)

                progressListener(progress, progressMessage)
            }
        }

        coroutineScope {
            val pipe = Channel<BlockInfo>()

            repeat(4 /* 4 blocks per time */) {
                workerNumber ->

                async {
                    for (block in pipe) {
                        logger.debug("request block with hash = {} from worker {}", block.hash, workerNumber)

                        blocksByHash[block.hash] = pullBlock(fileBlocks, block, 1000 * 15 /* 15 seconds timeout per block */)

                        updateProgress()
                    }
                }
            }

            fileBlocks.blocks.distinctBy { it.hash }.forEach {
                block -> pipe.send(block)
            }

            pipe.close()
        }

        val blockList = fileBlocks.blocks.map { blocksByHash[it.hash] }.toList()

        // TODO: clean up after stream close
        // TODO: clean up at error
        return SequenceInputStream(Collections.enumeration(blockList.map {ByteArrayInputStream(it) }))
    }

    private suspend fun pullBlock(fileBlocks: FileBlocks, block: BlockInfo, timeoutInMillis: Long): ByteArray {
        logger.debug("sent request for block, hash = {}", block.hash)

        val response =
                withTimeout(timeoutInMillis) {
                    doRequest(
                            Request.newBuilder()
                                    .setFolder(fileBlocks.folder)
                                    .setName(fileBlocks.path)
                                    .setOffset(block.offset)
                                    .setSize(block.size)
                                    .setHash(ByteString.copyFrom(Hex.decode(block.hash)))
                    )
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
        return suspendCancellableCoroutine {
            continuation ->

            val requestId = responseHandler.registerListener {
                response ->

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
