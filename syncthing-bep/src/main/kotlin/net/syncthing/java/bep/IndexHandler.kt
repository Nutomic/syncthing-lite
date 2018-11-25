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

import net.syncthing.java.core.beans.*
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.core.utils.trySubmitLogging
import org.apache.commons.lang3.tuple.Pair
import org.apache.http.util.TextUtils
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.Executors

class IndexHandler(private val configuration: Configuration, val indexRepository: IndexRepository,
                   private val tempRepository: TempRepository) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val folderInfoByFolder = mutableMapOf<String, FolderInfo>()
    private val indexMessageProcessor = IndexMessageProcessor()
    private var lastIndexActivity: Long = 0
    private val writeAccessLock = Object()
    private val indexWaitLock = Object()
    private val indexBrowsers = mutableSetOf<IndexBrowser>()
    private val onIndexRecordAcquiredListeners = mutableSetOf<(FolderInfo, List<FileInfo>, IndexInfo) -> Unit>()
    private val onFullIndexAcquiredListeners = mutableSetOf<(FolderInfo) -> Unit>()

    private fun lastActive(): Long = System.currentTimeMillis() - lastIndexActivity

    fun sequencer(): Sequencer = indexRepository.getSequencer()

    fun folderList(): List<String> = folderInfoByFolder.keys.toList()

    fun folderInfoList(): List<FolderInfo> = folderInfoByFolder.values.toList()

    private fun markActive() {
        lastIndexActivity = System.currentTimeMillis()
    }

    fun registerOnIndexRecordAcquiredListener(listener: (FolderInfo, List<FileInfo>, IndexInfo) -> Unit) {
        onIndexRecordAcquiredListeners.add(listener)
    }

    fun unregisterOnIndexRecordAcquiredListener(listener: (FolderInfo, List<FileInfo>, IndexInfo) -> Unit) {
        assert(onIndexRecordAcquiredListeners.contains(listener))
        onIndexRecordAcquiredListeners.remove(listener)
    }

    fun registerOnFullIndexAcquiredListenersListener(listener: (FolderInfo) -> Unit) {
        onFullIndexAcquiredListeners.add(listener)
    }

    fun unregisterOnFullIndexAcquiredListenersListener(listener: (FolderInfo) -> Unit) {
        assert(onFullIndexAcquiredListeners.contains(listener))
        onFullIndexAcquiredListeners.remove(listener)
    }

    init {
        loadFolderInfoFromConfig()
    }

    private fun loadFolderInfoFromConfig() {
        synchronized(writeAccessLock) {
            for (folderInfo in configuration.folders) {
                folderInfoByFolder.put(folderInfo.folderId, folderInfo) //TODO reference 'folder info' repository
            }
        }
    }

    @Synchronized
    fun clearIndex() {
        synchronized(writeAccessLock) {
            indexRepository.clearIndex()
            folderInfoByFolder.clear()
            loadFolderInfoFromConfig()
        }
    }

    internal fun isRemoteIndexAcquired(clusterConfigInfo: ConnectionHandler.ClusterConfigInfo, peerDeviceId: DeviceId): Boolean {
        var ready = true
        for (folder in clusterConfigInfo.getSharedFolders()) {
            val indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(peerDeviceId, folder)
            if (indexSequenceInfo == null || indexSequenceInfo.localSequence < indexSequenceInfo.maxSequence) {
                logger.debug("waiting for index on folder = {} sequenceInfo = {}", folder, indexSequenceInfo)
                ready = false
            }
        }
        return ready
    }

    @Throws(InterruptedException::class)
    fun waitForRemoteIndexAcquired(connectionHandler: ConnectionHandler, timeoutSecs: Long? = null): IndexHandler {
        val timeoutMillis = (timeoutSecs ?: DEFAULT_INDEX_TIMEOUT) * 1000
        synchronized(indexWaitLock) {
            while (!isRemoteIndexAcquired(connectionHandler.clusterConfigInfo!!, connectionHandler.deviceId())) {
                indexWaitLock.wait(timeoutMillis)
                NetworkUtils.assertProtocol(connectionHandler.getLastActive() < timeoutMillis || lastActive() < timeoutMillis,
                        {"unable to acquire index from connection $connectionHandler, timeout reached!"})
            }
        }
        logger.debug("acquired all indexes on connection {}", connectionHandler)
        return this
    }

    fun handleClusterConfigMessageProcessedEvent(clusterConfig: BlockExchangeProtos.ClusterConfig) {
        synchronized(writeAccessLock) {
            for (folderRecord in clusterConfig.foldersList) {
                val folder = folderRecord.id
                val folderInfo = updateFolderInfo(folder, folderRecord.label)
                logger.debug("acquired folder info from cluster config = {}", folderInfo)
                for (deviceRecord in folderRecord.devicesList) {
                    val deviceId = DeviceId.fromHashData(deviceRecord.id.toByteArray())
                    if (deviceRecord.hasIndexId() && deviceRecord.hasMaxSequence()) {
                        val folderIndexInfo = updateIndexInfo(folder, deviceId, deviceRecord.indexId, deviceRecord.maxSequence, null)
                        logger.debug("acquired folder index info from cluster config = {}", folderIndexInfo)
                    }
                }
            }
        }
    }

    fun handleIndexMessageReceivedEvent(folderId: String, filesList: List<BlockExchangeProtos.FileInfo>, connectionHandler: ConnectionHandler) {
        indexMessageProcessor.handleIndexMessageReceivedEvent(folderId, filesList, connectionHandler)
    }

    fun pushRecord(folder: String, bepFileInfo: BlockExchangeProtos.FileInfo): FileInfo? {
        var fileBlocks: FileBlocks? = null
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(bepFileInfo.name)
                .setLastModified(Date(bepFileInfo.modifiedS * 1000 + bepFileInfo.modifiedNs / 1000000))
                .setVersionList((if (bepFileInfo.hasVersion()) bepFileInfo.version.countersList else null ?: emptyList()).map { record -> Version(record.id, record.value) })
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
        return addRecord(builder.build(), fileBlocks)
    }

    private fun updateIndexInfo(folder: String, deviceId: DeviceId, indexId: Long?, maxSequence: Long?, localSequence: Long?): IndexInfo {
        synchronized(writeAccessLock) {
            var indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder)
            var shouldUpdate = false
            val builder: IndexInfo.Builder
            if (indexSequenceInfo == null) {
                shouldUpdate = true
                assert(indexId != null, {"index sequence info not found, and supplied null index id (folder = $folder, device = $deviceId)"})
                builder = IndexInfo.newBuilder()
                        .setFolder(folder)
                        .setDeviceId(deviceId.deviceId)
                        .setIndexId(indexId!!)
                        .setLocalSequence(0)
                        .setMaxSequence(-1)
            } else {
                builder = indexSequenceInfo.copyBuilder()
            }
            if (indexId != null && indexId != builder.getIndexId()) {
                shouldUpdate = true
                builder.setIndexId(indexId)
            }
            if (maxSequence != null && maxSequence > builder.getMaxSequence()) {
                shouldUpdate = true
                builder.setMaxSequence(maxSequence)
            }
            if (localSequence != null && localSequence > builder.getLocalSequence()) {
                shouldUpdate = true
                builder.setLocalSequence(localSequence)
            }
            if (shouldUpdate) {
                indexSequenceInfo = builder.build()
                indexRepository.updateIndexInfo(indexSequenceInfo)
            }
            return indexSequenceInfo!!
        }
    }

    private fun addRecord(record: FileInfo, fileBlocks: FileBlocks?): FileInfo? {
        synchronized(writeAccessLock) {
            val lastModified = indexRepository.findFileInfoLastModified(record.folder, record.path)
            return if (lastModified != null && record.lastModified < lastModified) {
                logger.trace("discarding record = {}, modified before local record", record)
                null
            } else {
                indexRepository.updateFileInfo(record, fileBlocks)
                logger.trace("loaded new record = {}", record)
                indexBrowsers.forEach {
                    it.onIndexChangedevent(record.folder)
                }
                record
            }
        }
    }

    fun getFileInfoByPath(folder: String, path: String): FileInfo? {
        return indexRepository.findFileInfo(folder, path)
    }

    fun getFileInfoAndBlocksByPath(folder: String, path: String): Pair<FileInfo, FileBlocks>? {
        val fileInfo = getFileInfoByPath(folder, path)
        return if (fileInfo == null) {
            null
        } else {
            assert(fileInfo.isFile())
            val fileBlocks = indexRepository.findFileBlocks(folder, path)
            checkNotNull(fileBlocks) {"file blocks not found for file info = $fileInfo"}

            FileInfo.checkBlocks(fileInfo, fileBlocks)

            Pair.of(fileInfo, fileBlocks)
        }
    }

    private fun updateFolderInfo(folder: String, label: String?): FolderInfo {
        var folderInfo: FolderInfo? = folderInfoByFolder[folder]
        if (folderInfo == null || !TextUtils.isEmpty(label)) {
            folderInfo = FolderInfo(folder, label)
            folderInfoByFolder.put(folderInfo.folderId, folderInfo)
        }
        return folderInfo
    }

    fun getFolderInfo(folder: String): FolderInfo? {
        return folderInfoByFolder[folder]
    }

    fun getIndexInfo(device: DeviceId, folder: String): IndexInfo? {
        return indexRepository.findIndexInfoByDeviceAndFolder(device, folder)
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
        assert(onIndexRecordAcquiredListeners.isEmpty())
        assert(onFullIndexAcquiredListeners.isEmpty())
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

        fun handleIndexMessageReceivedEvent(folderId: String, filesList: List<BlockExchangeProtos.FileInfo>, connectionHandler: ConnectionHandler) {
            logger.info("received index message event, preparing (queued records = {} event record count = {})", queuedRecords, filesList.size)
            markActive()
            val clusterConfigInfo = connectionHandler.clusterConfigInfo
            val peerDeviceId = connectionHandler.deviceId()
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

        private fun processBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ConnectionHandler.ClusterConfigInfo?, peerDeviceId: DeviceId) {
            logger.debug("received index message event, queuing for processing")
            queuedMessages++
            queuedRecords += data.filesCount.toLong()
            executorService.trySubmitLogging(object : ProcessingRunnable() {
                override fun runProcess() {
                    doHandleIndexMessageReceivedEvent(data, clusterConfigInfo, peerDeviceId)
                }
            })
        }

        private fun storeAndProcessBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ConnectionHandler.ClusterConfigInfo?, peerDeviceId: DeviceId) {
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
            protected fun doHandleIndexMessageReceivedEvent(key: String, clusterConfigInfo: ConnectionHandler.ClusterConfigInfo?, peerDeviceId: DeviceId) {
                logger.debug("processing index message event from temp record {}", key)
                markActive()
                val data = tempRepository.popTempData(key)
                val message = BlockExchangeProtos.IndexUpdate.parseFrom(data)
                doHandleIndexMessageReceivedEvent(message, clusterConfigInfo, peerDeviceId)
            }

            protected fun doHandleIndexMessageReceivedEvent(message: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: ConnectionHandler.ClusterConfigInfo?, peerDeviceId: DeviceId) {
                //            synchronized (writeAccessLock) {
                //                if (addProcessingDelayForInterface) {
                //                    delay = Math.min(MAX_DELAY, Math.max(MIN_DELAY, lastRecordProcessingTime * DELAY_FACTOR));
                //                    logger.info("add delay of {} secs before processing index message (to allow UI to process)", delay / 1000d);
                //                    try {
                //                        Thread.sleep(delay);
                //                    } catch (InterruptedException ex) {
                //                        logger.warn("interrupted", ex);
                //                    }
                //                } else {
                //                    delay = 0;
                //                }
                logger.info("processing index message with {} records (queue size: messages = {} records = {})", message.filesCount, queuedMessages, queuedRecords)
                //            String deviceId = connectionHandler.getDeviceId();
                val folderId = message.folder
                var sequence: Long = -1
                val newRecords = mutableListOf<FileInfo>()
                //                IndexInfo oldIndexInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder);
                //            Stopwatch stopwatch = Stopwatch.createStarted();
                logger.debug("processing {} index records for folder {}", message.filesList.size, folderId)
                for (fileInfo in message.filesList) {
                    markActive()
                    //                    if (oldIndexInfo != null && isVersionOlderThanSequence(fileInfo, oldIndexInfo.getLocalSequence())) {
                    //                        logger.trace("skipping file {}, version older than sequence {}", fileInfo, oldIndexInfo.getLocalSequence());
                    //                    } else {
                    val newRecord = pushRecord(folderId, fileInfo)
                    if (newRecord != null) {
                        newRecords.add(newRecord)
                    }
                    sequence = Math.max(fileInfo.sequence, sequence)
                    markActive()
                    //                    }
                }
                val newIndexInfo = updateIndexInfo(folderId, peerDeviceId, null, null, sequence)
                val elap = System.currentTimeMillis() - startTime!!
                queuedRecords -= message.filesCount.toLong()
                logger.info("processed {} index records, acquired {} ({} secs, {} record/sec)", message.filesCount, newRecords.size, elap / 1000.0, Math.round(message.filesCount / (elap / 1000.0) * 100) / 100.0)
                if (logger.isInfoEnabled && newRecords.size <= 10) {
                    for (fileInfo in newRecords) {
                        logger.info("acquired record = {}", fileInfo)
                    }
                }
                val folderInfo = folderInfoByFolder[folderId]
                if (!newRecords.isEmpty()) {
                    onIndexRecordAcquiredListeners.forEach { it(folderInfo!!, newRecords, newIndexInfo) }
                }
                logger.debug("index info = {}", newIndexInfo)
                if (isRemoteIndexAcquired(clusterConfigInfo!!, peerDeviceId)) {
                    logger.debug("index acquired")
                    onFullIndexAcquiredListeners.forEach { it(folderInfo!!)}
                }
                //                IndexHandler.this.notifyAll();
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
