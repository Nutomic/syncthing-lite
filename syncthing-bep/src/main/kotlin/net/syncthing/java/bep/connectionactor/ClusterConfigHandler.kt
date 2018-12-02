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
package net.syncthing.java.bep.connectionactor

import com.google.protobuf.ByteString
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.Configuration
import org.slf4j.LoggerFactory

object ClusterConfigHandler {
    private val logger = LoggerFactory.getLogger(ClusterConfigHandler::class.java)

    fun buildClusterConfig(
            configuration: Configuration,
            indexHandler: IndexHandler,
            deviceId: DeviceId
    ): BlockExchangeProtos.ClusterConfig {
        val builder = BlockExchangeProtos.ClusterConfig.newBuilder()

        indexHandler.indexRepository.runInTransaction { indexTransaction ->
            for (folder in configuration.folders) {
                val folderBuilder = BlockExchangeProtos.Folder.newBuilder()
                        .setId(folder.folderId)
                        .setLabel(folder.label)

                // add this device
                folderBuilder.addDevices(
                        BlockExchangeProtos.Device.newBuilder()
                                .setId(ByteString.copyFrom(configuration.localDeviceId.toHashData()))
                                .setIndexId(indexTransaction.getSequencer().indexId())
                                .setMaxSequence(indexTransaction.getSequencer().currentSequence())
                )

                // add other device
                val indexSequenceInfo = indexTransaction.findIndexInfoByDeviceAndFolder(deviceId, folder.folderId)

                folderBuilder.addDevices(
                        BlockExchangeProtos.Device.newBuilder()
                                .setId(ByteString.copyFrom(deviceId.toHashData()))
                                .apply {
                                    indexSequenceInfo?.let {
                                        setIndexId(indexSequenceInfo.indexId)
                                        setMaxSequence(indexSequenceInfo.localSequence)

                                        logger.info("send delta index info device = {} index = {} max (local) sequence = {}",
                                                indexSequenceInfo.deviceId,
                                                indexSequenceInfo.indexId,
                                                indexSequenceInfo.localSequence)
                                    }
                                }
                )

                builder.addFolders(folderBuilder)

                // TODO: add the other devices to the cluster config
            }
        }

        return builder.build()
    }

    // TODO: understand this
    internal suspend fun handleReceivedClusterConfig(
            clusterConfig: BlockExchangeProtos.ClusterConfig,
            configuration: Configuration,
            otherDeviceId: DeviceId,
            indexHandler: IndexHandler
    ): ClusterConfigInfo {
        val folderInfoList = mutableListOf<ClusterConfigFolderInfo>()
        val newSharedFolders = mutableListOf<FolderInfo>()

        for (folder in clusterConfig.foldersList ?: emptyList()) {
            var folderInfo = ClusterConfigFolderInfo(folder.id, folder.label)
            val devicesById = (folder.devicesList ?: emptyList())
                    .associateBy { input ->
                        DeviceId.fromHashData(input.id!!.toByteArray())
                    }
            val otherDevice = devicesById[otherDeviceId]
            val ourDevice = devicesById[configuration.localDeviceId]
            if (otherDevice != null) {
                folderInfo = folderInfo.copy(isAnnounced = true)
            }
            if (ourDevice != null) {
                folderInfo = folderInfo.copy(isShared = true)
                logger.info("folder shared from device = {} folder = {}", otherDeviceId, folderInfo)

                val newFolderInfo = FolderInfo(folderInfo.folderId, folderInfo.label)

                val oldFolderEntry = configuration.folders.find { it.folderId == folderInfo.folderId }

                if (oldFolderEntry == null) {
                    configuration.folders = configuration.folders + newFolderInfo
                    newSharedFolders.add(newFolderInfo)
                    logger.info("new folder shared = {}", folderInfo)
                } else {
                    if (oldFolderEntry != newFolderInfo) {
                        configuration.folders = configuration.folders.filter { it != oldFolderEntry }.toSet() + setOf(newFolderInfo)
                    }
                }
            } else {
                logger.info("folder not shared from device = {} folder = {}", otherDeviceId, folderInfo)
            }

            folderInfoList.add(folderInfo)
        }
        configuration.persistLater()
        indexHandler.handleClusterConfigMessageProcessedEvent(clusterConfig)

        return ClusterConfigInfo(folderInfoList, newSharedFolders)
    }
}

class ClusterConfigInfo (val folderInfo: List<ClusterConfigFolderInfo>, val newSharedFolders: List<FolderInfo>) {
    companion object {
        val dummy = ClusterConfigInfo(folderInfo = emptyList(), newSharedFolders = emptyList())
    }

    val folderInfoById = folderInfo.associateBy { it.folderId }
    val sharedFolderIds: Set<String> by lazy {
        folderInfo.filter { it.isShared }.map { it.folderId }.toSet()
    }
}

data class ClusterConfigFolderInfo(
        val folderId: String,
        val label: String = folderId,
        val isAnnounced: Boolean = false,
        val isShared: Boolean = false
) {
    init {
        assert(folderId.isNotEmpty())
    }
}
