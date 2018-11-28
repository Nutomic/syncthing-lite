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
package net.syncthing.java.bep.index

import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.runBlocking
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.FolderBrowser
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.bep.connectionactor.ClusterConfigInfo
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.core.utils.trySubmitLogging
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.Executors

data class IndexRecordAcquiredEvent(val folderInfo: FolderInfo, val files: List<FileInfo>, val indexInfo: IndexInfo)

class IndexHandler(private val configuration: Configuration, val indexRepository: IndexRepository,
                   private val tempRepository: TempRepository) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val indexMessageProcessor = IndexMessageProcessor()
    private var lastIndexActivity: Long = 0
    private val writeAccessLock = Object()  // TODO: remove this; the transactions should replace it
    private val indexWaitLock = Object()
    /* TODO: make this private again or remove it */ val indexBrowsers = mutableSetOf<IndexBrowser>()
    private val onIndexRecordAcquiredEvents = BroadcastChannel<IndexRecordAcquiredEvent>(capacity = 16)
    private val onFullIndexAcquiredEvents = BroadcastChannel<FolderInfo>(capacity = 16)

    fun subscribeToOnFullIndexAcquiredEvents() = onFullIndexAcquiredEvents.openSubscription()
    fun subscribeToOnIndexRecordAcquiredEvents() = onIndexRecordAcquiredEvents.openSubscription()

    private fun lastActive(): Long = System.currentTimeMillis() - lastIndexActivity

    fun getNextSequenceNumber() = indexRepository.runInTransaction { it.getSequencer().nextSequence() }

    @Deprecated(message = "use configuration instead")
    fun folderList(): List<String> = configuration.folders.map { it.folderId }

    @Deprecated(message = "use configuration instead")
    fun folderInfoList(): List<FolderInfo> = configuration.folders.toList()

    private fun markActive() {
        lastIndexActivity = System.currentTimeMillis()
    }

    @Synchronized
    fun clearIndex() {
        synchronized(writeAccessLock) {
            indexRepository.runInTransaction { it.clearIndex() }
        }
    }

    private fun isRemoteIndexAcquiredWithoutTransaction(clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId): Boolean {
        return indexRepository.runInTransaction { transaction -> isRemoteIndexAcquired(clusterConfigInfo, peerDeviceId, transaction) }
    }

    private fun isRemoteIndexAcquired(clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId, transaction: IndexTransaction): Boolean {
        return clusterConfigInfo.sharedFolderIds.find { sharedFolderId ->
            // try to find one folder which is not yet ready
            val indexSequenceInfo = transaction.findIndexInfoByDeviceAndFolder(peerDeviceId, sharedFolderId)

            indexSequenceInfo == null || indexSequenceInfo.localSequence < indexSequenceInfo.maxSequence
        } == null
    }

    @Throws(InterruptedException::class)
    fun waitForRemoteIndexAcquired(connectionHandler: ConnectionActorWrapper, timeoutSecs: Long? = null): IndexHandler {
        val timeoutMillis = (timeoutSecs ?: DEFAULT_INDEX_TIMEOUT) * 1000
        synchronized(indexWaitLock) {
            while (!isRemoteIndexAcquiredWithoutTransaction(connectionHandler.getClusterConfig(), connectionHandler.deviceId)) {
                indexWaitLock.wait(timeoutMillis)
                NetworkUtils.assertProtocol(/* TODO connectionHandler.getLastActive() < timeoutMillis || */ lastActive() < timeoutMillis,
                        {"unable to acquire index from connection $connectionHandler, timeout reached!"})
            }
        }
        logger.debug("acquired all indexes on connection {}", connectionHandler)
        return this
    }

    fun handleClusterConfigMessageProcessedEvent(clusterConfig: BlockExchangeProtos.ClusterConfig) {
        synchronized(writeAccessLock) {
            indexRepository.runInTransaction { transaction ->
                for (folderRecord in clusterConfig.foldersList) {
                    val folder = folderRecord.id
                    logger.debug("acquired folder info from cluster config = {}", folder)
                    for (deviceRecord in folderRecord.devicesList) {
                        val deviceId = DeviceId.fromHashData(deviceRecord.id.toByteArray())
                        if (deviceRecord.hasIndexId() && deviceRecord.hasMaxSequence()) {
                            val folderIndexInfo = UpdateIndexInfo.updateIndexInfo(transaction, folder, deviceId, deviceRecord.indexId, deviceRecord.maxSequence, null)
                            logger.debug("acquired folder index info from cluster config = {}", folderIndexInfo)
                        }
                    }
                }
            }
        }
    }

    internal fun handleIndexMessageReceivedEvent(folderId: String, filesList: List<BlockExchangeProtos.FileInfo>, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
        indexMessageProcessor.handleIndexMessageReceivedEvent(folderId, filesList, clusterConfigInfo, peerDeviceId)
    }

    fun getFileInfoByPath(folder: String, path: String): FileInfo? {
        return indexRepository.runInTransaction { it.findFileInfo(folder, path) }
    }

    fun getFileInfoAndBlocksByPath(folder: String, path: String): Pair<FileInfo, FileBlocks>? {
        return indexRepository.runInTransaction { transaction ->
            val fileInfo = transaction.findFileInfo(folder, path)

            if (fileInfo == null) {
                null
            } else {
                val fileBlocks = transaction.findFileBlocks(folder, path)

                assert(fileInfo.isFile())
                checkNotNull(fileBlocks) {"file blocks not found for file info = $fileInfo"}

                FileInfo.checkBlocks(fileInfo, fileBlocks)

                Pair.of(fileInfo, fileBlocks)
            }
        }
    }

    @Deprecated("use config instead")
    fun getFolderInfo(folder: String): FolderInfo? {
        return configuration.folders.find { it.folderId == folder }
    }

    fun getIndexInfo(device: DeviceId, folder: String): IndexInfo? {
        return indexRepository.runInTransaction { it.findIndexInfoByDeviceAndFolder(device, folder) }
    }

    fun newFolderBrowser(): FolderBrowser {
        return FolderBrowser(this)
    }

    fun newIndexBrowser(folder: String, includeParentInList: Boolean = false, allowParentInRoot: Boolean = false,
                        ordering: Comparator<FileInfo>? = null): IndexBrowser {
        val indexBrowser = IndexBrowser(indexRepository, this, folder, includeParentInList, allowParentInRoot, ordering)
        indexBrowsers.add(indexBrowser)
        return indexBrowser
    }

    internal fun unregisterIndexBrowser(indexBrowser: IndexBrowser) {
        assert(indexBrowsers.contains(indexBrowser))
        indexBrowsers.remove(indexBrowser)
    }

    override fun close() {
        assert(indexBrowsers.isEmpty())
        onIndexRecordAcquiredEvents.close()
        onFullIndexAcquiredEvents.close()
        indexMessageProcessor.stop()
    }

    private inner class IndexMessageProcessor {

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
                            markActive = this@IndexHandler::markActive
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

    companion object {

        private const val DEFAULT_INDEX_TIMEOUT: Long = 30
    }
}
