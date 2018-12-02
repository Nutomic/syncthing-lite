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
import kotlinx.coroutines.channels.consume
import kotlinx.coroutines.withTimeoutOrNull
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.connectionactor.ClusterConfigInfo
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.bep.folder.FolderBrowser
import net.syncthing.java.bep.index.browser.IndexBrowser
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.interfaces.TempRepository
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException

data class IndexRecordAcquiredEvent(val folderId: String, val files: List<FileInfo>, val indexInfo: IndexInfo)

class IndexHandler(
        configuration: Configuration,
        val indexRepository: IndexRepository,
        tempRepository: TempRepository,
        enableDetailedException: Boolean
) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val onIndexRecordAcquiredEvents = BroadcastChannel<IndexRecordAcquiredEvent>(capacity = 16)
    private val onFullIndexAcquiredEvents = BroadcastChannel<String>(capacity = 16)
    private val onFolderStatsUpdatedEvents = BroadcastChannel<FolderStats>(capacity = 16)

    private val indexMessageProcessor = IndexMessageQueueProcessor(
            indexRepository = indexRepository,
            tempRepository = tempRepository,
            isRemoteIndexAcquired = ::isRemoteIndexAcquired,
            onIndexRecordAcquiredEvents = onIndexRecordAcquiredEvents,
            onFullIndexAcquiredEvents = onFullIndexAcquiredEvents,
            onFolderStatsUpdatedEvents = onFolderStatsUpdatedEvents,
            enableDetailedException = enableDetailedException
    )

    fun subscribeToOnFullIndexAcquiredEvents() = onFullIndexAcquiredEvents.openSubscription()
    fun subscribeToOnIndexRecordAcquiredEvents() = onIndexRecordAcquiredEvents.openSubscription()
    fun subscribeFolderStatsUpdatedEvents() = onFolderStatsUpdatedEvents.openSubscription()

    fun getNextSequenceNumber() = indexRepository.runInTransaction { it.getSequencer().nextSequence() }

    fun clearIndex() {
        indexRepository.runInTransaction { it.clearIndex() }
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

    // the old implementation kept waiting when index updates were still happening, but waiting 30 seconds should be enough
    suspend fun waitForRemoteIndexAcquiredWithTimeout(connectionHandler: ConnectionActorWrapper, timeoutSecs: Long? = null): IndexHandler {
        val timeoutMillis = (timeoutSecs ?: DEFAULT_INDEX_TIMEOUT) * 1000

        val ok = withTimeoutOrNull(timeoutMillis) {
            waitForRemoteIndexAcquiredWithoutTimeout(connectionHandler)

            true
        } ?: false

        if (!ok) {
            throw IOException("unable to acquire index from connection $connectionHandler, timeout reached!")
        }

        return this
    }

    suspend fun waitForRemoteIndexAcquiredWithoutTimeout(connectionHandler: ConnectionActorWrapper) {
        val events = onFullIndexAcquiredEvents.openSubscription()

        events.consume {
            fun isDone() = isRemoteIndexAcquiredWithoutTransaction(connectionHandler.getClusterConfig(), connectionHandler.deviceId)

            if (isDone()) {
                return
            }

            for (event in events) {
                if (isDone()) {
                    return
                }
            }
        }
    }

    suspend fun handleClusterConfigMessageProcessedEvent(clusterConfig: BlockExchangeProtos.ClusterConfig) {
        val updatedIndexInfos = indexRepository.runInTransaction { transaction ->
            val updatedIndexInfos = mutableListOf<IndexInfo>()

            for (folderRecord in clusterConfig.foldersList) {
                val folder = folderRecord.id
                logger.debug("acquired folder info from cluster config = {}", folder)
                for (deviceRecord in folderRecord.devicesList) {
                    val deviceId = DeviceId.fromHashData(deviceRecord.id.toByteArray())
                    if (deviceRecord.hasIndexId() && deviceRecord.hasMaxSequence()) {
                        val folderIndexInfo = UpdateIndexInfo.updateIndexInfo(transaction, folder, deviceId, deviceRecord.indexId, deviceRecord.maxSequence, null)
                        logger.debug("acquired folder index info from cluster config = {}", folderIndexInfo)
                        updatedIndexInfos.add(folderIndexInfo)
                    }
                }
            }

            updatedIndexInfos
        }

        updatedIndexInfos.forEach {
            onIndexRecordAcquiredEvents.send(
                    IndexRecordAcquiredEvent(
                            folderId = it.folderId,
                            indexInfo = it,
                            files = emptyList()
                    )
            )
        }
    }

    internal suspend fun handleIndexMessageReceivedEvent(folderId: String, filesList: List<BlockExchangeProtos.FileInfo>, clusterConfigInfo: ClusterConfigInfo, peerDeviceId: DeviceId) {
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

    val folderBrowser = FolderBrowser(this, configuration)
    val indexBrowser = IndexBrowser(indexRepository, this)

    suspend fun sendFolderStatsUpdate(event: FolderStats) {
        onFolderStatsUpdatedEvents.send(event)
    }

    override fun close() {
        onIndexRecordAcquiredEvents.close()
        onFullIndexAcquiredEvents.close()
        indexMessageProcessor.stop()
    }

    companion object {

        private const val DEFAULT_INDEX_TIMEOUT: Long = 30
    }
}
