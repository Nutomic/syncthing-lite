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
package net.syncthing.java.bep.index

import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.runBlocking
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.bep.connectionactor.ClusterConfigInfo
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.core.utils.trySubmitLogging
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.Executors

class IndexMessageProcessor (
        private val markActive: () -> Unit,
        private val indexRepository: IndexRepository,
        private val tempRepository: TempRepository,
        private val indexBrowsers: Set<IndexBrowser>,
        private val configuration: Configuration,
        private val onIndexRecordAcquiredEvents: BroadcastChannel<IndexRecordAcquiredEvent>,
        private val onFullIndexAcquiredEvents: BroadcastChannel<FolderInfo>,
        private val indexWaitLock: Object,
        private val isRemoteIndexAcquired: (ClusterConfigInfo, DeviceId, IndexTransaction) -> Boolean
) {
    companion object {
        private val logger = LoggerFactory.getLogger(IndexMessageProcessor::class.java)
    }

    private val executorService = Executors.newSingleThreadExecutor()
    private var queuedMessages = 0
    private var queuedRecords: Long = 0
    //        private long lastRecordProcessingTime = 0;
    //        , delay = 0;
    //        private boolean addProcessingDelayForInterface = true;
    //        private final int MIN_DELAY = 0, MAX_DELAY = 5000, MAX_RECORD_PER_PROCESS = 16, DELAY_FACTOR = 1;
    private var startTime: Long? = null

    fun handleIndexMessageReceivedEvent(folderId: String, filesList: List<BlockExchangeProtos.FileInfo>, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
        logger.info("received index message event, preparing (queued records = {} event record count = {})", queuedRecords, filesList.size)
        markActive()
        //            List<BlockExchangeProtos.FileInfo> fileList = event.getFilesList();
        //            for (int index = 0; index < fileList.size(); index += MAX_RECORD_PER_PROCESS) {
        //                BlockExchangeProtos.IndexUpdate data = BlockExchangeProtos.IndexUpdate.newBuilder()
        //                    .addAllFiles(Iterables.limit(Iterables.skip(fileList, index), MAX_RECORD_PER_PROCESS))
        //                    .setFolder(event.getFolder())
        //                    .build();
        //                if (queuedMessages > 0) {
        //                    storeAndProcessBg(data, clusterConfigInfo, peerDeviceId);
        //                } else {
        //                    processBg(data, clusterConfigInfo, peerDeviceId);
        //                }
        //            }
        val data = BlockExchangeProtos.IndexUpdate.newBuilder()
                .addAllFiles(filesList)
                .setFolder(folderId)
                .build()
        if (queuedMessages > 0) {
            storeAndProcessBg(data, clusterConfigInfo, peerDeviceId)
        } else {
            processBg(data, clusterConfigInfo, peerDeviceId)
        }
    }

    private fun processBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
        logger.debug("received index message event, queuing for processing")
        queuedMessages++
        queuedRecords += data.filesCount.toLong()
        executorService.trySubmitLogging(object : ProcessingRunnable() {
            override fun runProcess() {
                doHandleIndexMessageReceivedEvent(data, clusterConfigInfo, peerDeviceId)
            }
        })
    }

    private fun storeAndProcessBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
        val key = tempRepository.pushTempData(data.toByteArray())
        logger.debug("received index message event, stored to temp record {}, queuing for processing", key)
        queuedMessages++
        queuedRecords += data.filesCount.toLong()
        executorService.trySubmitLogging(object : ProcessingRunnable() {
            override fun runProcess() {
                try {
                    doHandleIndexMessageReceivedEvent(key, clusterConfigInfo, peerDeviceId)
                } catch (ex: IOException) {
                    logger.error("error processing index message", ex)
                }

            }

        })
    }

    private abstract inner class ProcessingRunnable : Runnable {

        override fun run() {
            startTime = System.currentTimeMillis()
            runProcess()
            queuedMessages--
            //                lastRecordProcessingTime = stopwatch.elapsed(TimeUnit.MILLISECONDS) - delay;
            //                logger.info("processed a bunch of records, {}*{} remaining", queuedMessages, MAX_RECORD_PER_PROCESS);
            //                logger.debug("processed index message in {} secs", lastRecordProcessingTime / 1000d);
            startTime = null
        }

        protected abstract fun runProcess()

        //        private boolean isVersionOlderThanSequence(BlockExchangeProtos.FileInfo fileInfo, long localSequence) {
        //            long fileSequence = fileInfo.getSequence();
        //            //TODO should we check last version instead of sequence? verify
        //            return fileSequence < localSequence;
        //        }
        @Throws(IOException::class)
        protected fun doHandleIndexMessageReceivedEvent(key: String, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
            logger.debug("processing index message event from temp record {}", key)
            markActive()
            val data = tempRepository.popTempData(key)
            val message = BlockExchangeProtos.IndexUpdate.parseFrom(data)
            doHandleIndexMessageReceivedEvent(message, clusterConfigInfo, peerDeviceId)
        }

        protected fun doHandleIndexMessageReceivedEvent(message: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
            logger.info("processing index message with {} records", message.filesCount)

            indexRepository.runInTransaction { indexTransaction ->
                val wasIndexAcquiredBefore = isRemoteIndexAcquired(clusterConfigInfo, peerDeviceId, indexTransaction)

                val (newIndexInfo, newRecords) = NewIndexMessageProcessor.doHandleIndexMessageReceivedEvent(
                        message = message,
                        peerDeviceId = peerDeviceId,
                        indexBrowsers = indexBrowsers,
                        transaction = indexTransaction,
                        markActive = markActive
                )

                logger.info("processed {} index records, acquired {}", message.filesCount, newRecords.size)

                val folderInfo = configuration.folders.find { it.folderId == message.folder } ?: FolderInfo(
                        folderId = message.folder,
                        label = message.folder
                )

                if (!newRecords.isEmpty()) {
                    runBlocking { onIndexRecordAcquiredEvents.send(IndexRecordAcquiredEvent(folderInfo, newRecords, newIndexInfo)) }
                }

                logger.debug("index info = {}", newIndexInfo)

                if (!wasIndexAcquiredBefore) {
                    if (isRemoteIndexAcquired(clusterConfigInfo, peerDeviceId, indexTransaction)) {
                        logger.debug("index acquired")
                        runBlocking { onFullIndexAcquiredEvents.send(folderInfo) }
                    }
                }
            }

            markActive()

            synchronized(indexWaitLock) {
                indexWaitLock.notifyAll()
            }
        }
    }

    fun stop() {
        logger.info("stopping index record processor")
        executorService.shutdown()
        executorService.awaitTerminationSafe()
    }

}
