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
import net.syncthing.java.bep.ClusterConfigFolderInfo
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.Configuration
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

object ClusterConfigHandler {
    private val logger = LoggerFactory.getLogger(ClusterConfigHandler::class.java)

    fun buildClusterConfig(
            configuration: Configuration,
            indexHandler: IndexHandler,
            deviceId: DeviceId
    ): BlockExchangeProtos.ClusterConfig {
        val builder = BlockExchangeProtos.ClusterConfig.newBuilder()

        for (folder in configuration.folders) {
            val folderBuilder = BlockExchangeProtos.Folder.newBuilder()
                    .setId(folder.folderId)
                    .setLabel(folder.label)

            // add this device
            folderBuilder.addDevices(
                    BlockExchangeProtos.Device.newBuilder()
                            .setId(ByteString.copyFrom(configuration.localDeviceId.toHashData()))
                            .setIndexId(indexHandler.sequencer().indexId())
                            .setMaxSequence(indexHandler.sequencer().currentSequence())
            )

            // add other device
            val indexSequenceInfo = indexHandler.indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder.folderId)

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

        return builder.build()
    }

    // TODO: understand this
    internal fun handleReceivedClusterConfig(
            clusterConfig: BlockExchangeProtos.ClusterConfig,
            configuration: Configuration,
            otherDeviceId: DeviceId,
            indexHandler: IndexHandler,
            onNewFolderSharedListener: (FolderInfo) -> Unit
    ): ClusterConfigInfo {
        val clusterConfigInfo = ClusterConfigInfo()

        for (folder in clusterConfig.foldersList ?: emptyList()) {
            val folderInfo = ClusterConfigFolderInfo(folder.id, folder.label)
            val devicesById = (folder.devicesList ?: emptyList())
                    .associateBy { input ->
                        DeviceId.fromHashData(input.id!!.toByteArray())
                    }
            val otherDevice = devicesById[otherDeviceId]
            val ourDevice = devicesById[configuration.localDeviceId]
            if (otherDevice != null) {
                folderInfo.isAnnounced = true
            }
            if (ourDevice != null) {
                folderInfo.isShared = true
                logger.info("folder shared from device = {} folder = {}", otherDeviceId, folderInfo)
                val folderIds = configuration.folders.map { it.folderId }
                if (!folderIds.contains(folderInfo.folderId)) {
                    val fi = FolderInfo(folderInfo.folderId, folderInfo.label)
                    configuration.folders = configuration.folders + fi
                    onNewFolderSharedListener(fi)
                    logger.info("new folder shared = {}", folderInfo)
                }
            } else {
                logger.info("folder not shared from device = {} folder = {}", otherDeviceId, folderInfo)
            }
            clusterConfigInfo.putFolderInfo(folderInfo)
        }
        configuration.persistLater()
        indexHandler.handleClusterConfigMessageProcessedEvent(clusterConfig)

        return clusterConfigInfo
    }
}

class ClusterConfigInfo {
    private val folderInfoById = ConcurrentHashMap<String, ClusterConfigFolderInfo>()

    fun getSharedFolders(): Set<String> = folderInfoById.values.filter { it.isShared }.map { it.folderId }.toSet()

    fun putFolderInfo(folderInfo: ClusterConfigFolderInfo) {
        folderInfoById[folderInfo.folderId] = folderInfo
    }
}
