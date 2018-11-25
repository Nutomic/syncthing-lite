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
import net.syncthing.java.bep.*
import net.syncthing.java.bep.connectionactor.ConnectionActorGenerator
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.discovery.DiscoveryHandler
import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.util.Collections
import java.util.TreeSet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class SyncthingClient(
        private val configuration: Configuration,
        private val repository: IndexRepository,
        private val tempRepository: TempRepository
) : Closeable {

<<<<<<< HEAD
    val indexHandler = IndexHandler(configuration, repository, tempRepository)
    val discoveryHandler = DiscoveryHandler(configuration)
=======
    private val logger = LoggerFactory.getLogger(javaClass)
    val discoveryHandler: DiscoveryHandler
    val indexHandler: IndexHandler
    private val connections = Collections.synchronizedSet(createConnectionsSet())
    private val connectByDeviceIdLocks = Collections.synchronizedMap(HashMap<DeviceId, Any>())
    private val onConnectionChangedListeners = Collections.synchronizedList(mutableListOf<(DeviceId) -> Unit>())
    private var connectDevicesScheduler = Executors.newSingleThreadScheduledExecutor()
>>>>>>> master

    private val onConnectionChangedListeners = Collections.synchronizedList(mutableListOf<(DeviceId) -> Unit>())

    private val requestHandlerRegistry = RequestHandlerRegistry()
    private val newConnections = Connections(
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
        // TODO: update index from peers by reconnecting
    }

    fun addOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        onConnectionChangedListeners.add(listener)
    }

    fun removeOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        assert(onConnectionChangedListeners.contains(listener))
        onConnectionChangedListeners.remove(listener)
    }

<<<<<<< HEAD
    private fun getConnections() = configuration.peerIds.map { newConnections.getByDeviceId(it) }
=======
    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    private fun openConnection(deviceAddress: DeviceAddress): ConnectionHandler {
        logger.debug("Connecting to ${deviceAddress.deviceId}, active connections: ${connections.map { it.deviceId().deviceId }}")
        val connectionHandler = ConnectionHandler(
                configuration, deviceAddress, indexHandler, tempRepository, { connectionHandler, _ ->
                    connectionHandler.close()
                    openConnection(deviceAddress)
                },
                {connection ->
                    if (!connection.isConnected) {
                        connections.remove(connection)
                    }
                    onConnectionChangedListeners.forEach { it(connection.deviceId()) }
                })

        try {
          connectionHandler.connect()
        } catch (ex: Exception) {
          connectionHandler.closeBg()

          throw ex
        }

        connections.add(connectionHandler)

        return connectionHandler
    }

    /**
     * Takes discovered addresses from [[DiscoveryHandler]] and connects to devices.
     *
     * We need to make sure that we are only connecting once to each device.
     */
    private fun getPeerConnections(listener: (connection: ConnectionHandler) -> Unit, completeListener: () -> Unit) {
        // create an copy to prevent dispatching an action two times
        val connectionsWhichWereDispatched = createConnectionsSet()

        synchronized (connections) {
          connectionsWhichWereDispatched.addAll(connections)
        }

        connectionsWhichWereDispatched.forEach { listener(it) }

        discoveryHandler.newDeviceAddressSupplier()
                .takeWhile { it != null }
                .filterNotNull()
                .groupBy { it.deviceId }
                .filterNot { it.value.isEmpty() }
                .forEach { (deviceId, addresses) ->
                    // create an lock per device id to prevent multiple connections to one device

                    synchronized (connectByDeviceIdLocks) {
                      if (connectByDeviceIdLocks[deviceId] == null) {
                        connectByDeviceIdLocks[deviceId] = Object()
                      }
                    }

                    synchronized (connectByDeviceIdLocks[deviceId]!!) {
                      val existingConnection = connections.find { it.deviceId() == deviceId && it.isConnected }

                      if (existingConnection != null) {
                        connectionsWhichWereDispatched.add(existingConnection)
                        listener(existingConnection)

                        return@synchronized
                      }

                      // try to use all addresses
                      for (address in addresses.distinctBy { it.address }) {
                        try {
                          val newConnection = openConnection(address)

                          connectionsWhichWereDispatched.add(newConnection)
                          listener(newConnection)

                          break  // it worked, no need to try more
                        } catch (e: IOException) {
                          logger.warn("error connecting to device = $address", e)
                        } catch (e: KeystoreHandler.CryptoException) {
                          logger.warn("error connecting to device = $address", e)
                        }
                      }
                    }
                }

        // use all connections which were added in the time between and were not added by this function call
        val newConnectionsBackup = createConnectionsSet()

        synchronized (connections) {
          newConnectionsBackup.addAll(connections)
        }

        connectionsWhichWereDispatched.forEach { newConnectionsBackup.remove(it) }

        newConnectionsBackup.forEach { listener(it) }

        completeListener()
    }

    private fun updateIndexFromPeers() {
        getPeerConnections({ connection ->
            try {
                indexHandler.waitForRemoteIndexAcquired(connection)
            } catch (ex: InterruptedException) {
                logger.warn("exception while waiting for index", ex)
            }
        }, {})
    }

    private fun getConnectionForFolder(folder: String, listener: (connection: ConnectionHandler) -> Unit,
                                       errorListener: () -> Unit) {
        val isConnected = AtomicBoolean(false)
        getPeerConnections({ connection ->
            if (connection.hasFolder(folder) && !isConnected.get()) {
                listener(connection)
                isConnected.set(true)
            }
        }, {
            if (!isConnected.get()) {
                errorListener()
            }
        })
    }
>>>>>>> master

    init {
        discoveryHandler.newDeviceAddressSupplier() // starts the discovery
        getConnections()
    }

    fun getActiveConnectionsForFolder(folderId: String) = configuration.peerIds
            .map { newConnections.getByDeviceId(it) }
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

    fun getPeerStatus() = configuration.peers.map { device ->
        device.copy(
                isConnected = newConnections.getByDeviceId(device.deviceId).isConnected
        )
    }

    override fun close() {
        discoveryHandler.close()
        indexHandler.close()
        repository.close()
        tempRepository.close()
        newConnections.shutdown()
        assert(onConnectionChangedListeners.isEmpty())
    }
}
