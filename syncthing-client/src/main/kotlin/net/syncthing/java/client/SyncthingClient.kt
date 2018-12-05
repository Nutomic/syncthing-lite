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
package net.syncthing.java.client

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.bep.BlockPullerStatus
import net.syncthing.java.bep.BlockPusher
import net.syncthing.java.bep.RequestHandlerRegistry
import net.syncthing.java.bep.connectionactor.ConnectionActorGenerator
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.discovery.DiscoveryHandler
import java.io.Closeable
import java.io.InputStream
import java.util.*

class SyncthingClient(
        private val configuration: Configuration,
        private val repository: IndexRepository,
        private val tempRepository: TempRepository,
        enableDetailedException: Boolean = false
) : Closeable {
    val indexHandler = IndexHandler(configuration, repository, tempRepository, enableDetailedException)
    val discoveryHandler = DiscoveryHandler(configuration)
    private val onConnectionChangedListeners = Collections.synchronizedList(mutableListOf<(DeviceId) -> Unit>())

    private val requestHandlerRegistry = RequestHandlerRegistry()
    private val connections = Connections(
            generate = { deviceId ->
                ConnectionActorWrapper(
                        source = ConnectionActorGenerator.generateConnectionActors(
                                deviceAddress = discoveryHandler.devicesAddressesManager.getDeviceAddressManager(deviceId).streamCurrentDeviceAddresses(),
                                requestHandler = { request ->
                                    GlobalScope.async {
                                        requestHandlerRegistry.handleRequest(
                                                source = deviceId,
                                                request = request
                                        )
                                    }
                                },
                                indexHandler = indexHandler,
                                configuration = configuration
                        ),
                        deviceId = deviceId,
                        connectivityChangeListener = {
                            synchronized(onConnectionChangedListeners) {
                                onConnectionChangedListeners.forEach { it(deviceId) }
                            }
                        }
                )
            }
    )

    fun clearCacheAndIndex() {
        indexHandler.clearIndex()
        configuration.folders = emptySet()
        configuration.persistLater()
        connections.reconnectAllConnections()
    }

    // TODO: remove these callbacks
    fun addOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        onConnectionChangedListeners.add(listener)
    }

    fun removeOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        assert(onConnectionChangedListeners.contains(listener))
        onConnectionChangedListeners.remove(listener)
    }

    private fun getConnections() = configuration.peerIds.map { connections.getByDeviceId(it) }

    init {
        discoveryHandler.newDeviceAddressSupplier() // starts the discovery
        getConnections()
    }

    fun reconnect(deviceId: DeviceId) {
        connections.reconnect(deviceId)
    }

    fun connectToNewlyAddedDevices() {
        getConnections()
    }

    fun disconnectFromRemovedDevices() {
        // TODO: implement this
    }

    fun getActiveConnectionsForFolder(folderId: String) = configuration.peerIds
            .map { connections.getByDeviceId(it) }
            .filter { it.isConnected && it.hasFolder(folderId) }

    suspend fun pullFile(
            fileInfo: FileInfo,
            progressListener: (status: BlockPullerStatus) -> Unit = {  }
    ): InputStream = BlockPuller.pullFile(
            fileInfo = fileInfo,
            progressListener = progressListener,
            connections = getConnections(),
            indexHandler = indexHandler,
            tempRepository = tempRepository
    )

    fun pullFileSync(fileInfo: FileInfo) = runBlocking { pullFile(fileInfo) }

    fun getBlockPusher(folderId: String): BlockPusher {
        val connection = getActiveConnectionsForFolder(folderId).first()

        return BlockPusher(
                localDeviceId = connection.deviceId,
                connectionHandler = connection,
                indexHandler = indexHandler,
                requestHandlerRegistry = requestHandlerRegistry
        )
    }

    fun subscribeToConnectionStatus() = connections.subscribeToConnectionStatusMap()

    override fun close() {
        discoveryHandler.close()
        indexHandler.close()
        repository.close()
        tempRepository.close()
        connections.shutdown()
        assert(onConnectionChangedListeners.isEmpty())
    }
}
