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

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
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
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*

data class IndexRecordAcquiredEvent(val folderInfo: FolderInfo, val files: List<FileInfo>, val indexInfo: IndexInfo)

class IndexHandler(private val configuration: Configuration, val indexRepository: IndexRepository,
                   private val tempRepository: TempRepository) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private var lastIndexActivity: Long = 0
    private val indexWaitLock = Object()
    private val indexBrowsers = mutableSetOf<IndexBrowser>()
    private val onIndexRecordAcquiredEvents = BroadcastChannel<IndexRecordAcquiredEvent>(capacity = 16)
    private val onFullIndexAcquiredEvents = BroadcastChannel<FolderInfo>(capacity = 16)

    init {
        GlobalScope.launch {
            onIndexRecordAcquiredEvents.openSubscription().consumeEach {
                indexBrowsers.forEach { it.onIndexChangedevent(it.folder) }
            }
        }
    }

    private val indexMessageProcessor = IndexMessageQueueProcessor(
            indexRepository = indexRepository,
            markActive = ::markActive,
            tempRepository = tempRepository,
            indexWaitLock = indexWaitLock,
            isRemoteIndexAcquired = ::isRemoteIndexAcquired,
            onIndexRecordAcquiredEvents = onIndexRecordAcquiredEvents,
            onFullIndexAcquiredEvents = onFullIndexAcquiredEvents,
            configuration = configuration
    )

    fun subscribeToOnFullIndexAcquiredEvents() = onFullIndexAcquiredEvents.openSubscription()
    fun subscribeToOnIndexRecordAcquiredEvents() = onIndexRecordAcquiredEvents.openSubscription()

    private fun lastActive(): Long = System.currentTimeMillis() - lastIndexActivity

    fun getNextSequenceNumber() = indexRepository.runInTransaction { it.getSequencer().nextSequence() }

    @Deprecated(message = "use configuration instead")
    fun folderInfoList(): List<FolderInfo> = configuration.folders.toList()

    private fun markActive() {
        lastIndexActivity = System.currentTimeMillis()
    }

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

    // FIXME: there should only be one instance of this
    fun newFolderBrowser(): FolderBrowser {
        return FolderBrowser(this, configuration)
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

    companion object {

        private const val DEFAULT_INDEX_TIMEOUT: Long = 30
    }
}
