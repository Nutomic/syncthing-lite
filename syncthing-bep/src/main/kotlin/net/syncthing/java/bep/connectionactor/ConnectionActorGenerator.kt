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

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import org.slf4j.LoggerFactory
import java.io.IOException

object ConnectionActorGenerator {
    private val closed = Channel<ConnectionAction>().apply { cancel() }
    private val logger = LoggerFactory.getLogger(ConnectionActorGenerator::class.java)

    private fun deviceAddressesGenerator(deviceAddress: ReceiveChannel<DeviceAddress>) = GlobalScope.produce<List<DeviceAddress>> (capacity = Channel.CONFLATED) {
        val addresses = mutableMapOf<String, DeviceAddress>()

        deviceAddress.consumeEach { address ->
            val isNew = addresses[address.address] == null

            addresses[address.address] = address

            if (isNew) {
                send(
                        addresses.values.sortedBy { it.score }
                )
            }
        }
    }

    private fun <T> waitForFirstValue(source: ReceiveChannel<T>, time: Long) = GlobalScope.produce<T> {
        source.consume {
            val firstValue = source.receive()
            var lastValue = firstValue

            try {
                withTimeout(time) {
                    while (true) {
                        lastValue = source.receive()
                    }
                }

                throw IllegalStateException()
            } catch (ex: TimeoutCancellationException) {
                // this is expected here
            }

            send(lastValue)

            // other values without delay
            for (value in source) {
                send(value)
            }
        }
    }

    fun generateConnectionActors(
            deviceAddress: ReceiveChannel<DeviceAddress>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = generateConnectionActorsFromDeviceAddressList(
            deviceAddressSource = waitForFirstValue(
                    source = deviceAddressesGenerator(deviceAddress),
                    time = 1000
            ),
            configuration = configuration,
            indexHandler = indexHandler,
            requestHandler = requestHandler
    )

    fun generateConnectionActorsFromDeviceAddressList(
            deviceAddressSource: ReceiveChannel<List<DeviceAddress>>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = GlobalScope.produce<Pair<SendChannel<ConnectionAction>, ClusterConfigInfo>> {
        var currentActor: SendChannel<ConnectionAction> = closed
        var currentDeviceAddress: DeviceAddress? = null

        suspend fun closeCurrent() {
            if (currentActor != closed) {
                currentActor.close()
                currentActor = closed
                send(currentActor to ClusterConfigInfo.dummy)
            }
        }

        suspend fun tryConnectingToAddressHandleBaseErrors(deviceAddress: DeviceAddress) = try {
            val newActor = ConnectionActor.createInstance(deviceAddress, configuration, indexHandler, requestHandler)
            val clusterConfig = ConnectionActorUtil.waitUntilConnected(newActor)

            newActor to clusterConfig
        } catch (ex: Exception) {
            logger.warn("failed to connect to $deviceAddress", ex)

            when (ex) {
                is IOException -> {/* expected -> ignore */}
                is InterruptedException -> {/* expected -> ignore */}
                else -> throw ex
            }

            null
        }

        suspend fun dispatchConnection(
                connection: SendChannel<ConnectionAction>,
                clusterConfig: ClusterConfigInfo,
                deviceAddress: DeviceAddress
        ) {
            currentActor = connection
            currentDeviceAddress = deviceAddress

            send(connection to clusterConfig)
        }

        suspend fun tryConnectingToAddress(deviceAddress: DeviceAddress): Boolean {
            closeCurrent()

            var connection = tryConnectingToAddressHandleBaseErrors(deviceAddress) ?: return false

            if (connection.second.newSharedFolders.isNotEmpty()) {
                logger.debug("connected to $deviceAddress with new folders -> reconnect")
                // reconnect to send new cluster config
                connection.first.close()
                connection = tryConnectingToAddressHandleBaseErrors(deviceAddress) ?: return false
            }

            logger.debug("connected to $deviceAddress")

            dispatchConnection(connection.first, connection.second, deviceAddress)

            return true
        }

        fun isConnected() = !currentActor.isClosedForSend

        invokeOnClose {
            currentActor.close()
        }

        val reconnectTicker = ticker(delayMillis = 30 * 1000, initialDelayMillis = 0)

        deviceAddressSource.consume {
            var lastDeviceAddressList: List<DeviceAddress> = emptyList()

            while (true) {
                if (isConnected()) {
                    lastDeviceAddressList = deviceAddressSource.poll() ?: lastDeviceAddressList

                    if (lastDeviceAddressList.isNotEmpty()) {
                        if (reconnectTicker.poll() != null) {
                            if (currentDeviceAddress != lastDeviceAddressList.first()) {
                                val oldDeviceAddress = currentDeviceAddress!!

                                if (!tryConnectingToAddress(lastDeviceAddressList.first())) {
                                    tryConnectingToAddress(oldDeviceAddress)
                                }
                            }
                        }
                    } else {
                        closeCurrent()
                    }

                    delay(500)  // don't take too much CPU
                } else /* is not connected */ {
                    // get the new list version if there is any
                    lastDeviceAddressList = deviceAddressSource.poll() ?: lastDeviceAddressList

                    // try all addresses
                    for (address in lastDeviceAddressList) {
                        if (tryConnectingToAddress(address)) {
                            break
                        }
                    }

                    // reset countdown before trying other connection if it would be time now
                    // this does not reset if it has not counted down the whole time yet
                    reconnectTicker.poll()

                    // wait for new device address list but not more than 15 seconds before the next iteration
                    lastDeviceAddressList = withTimeoutOrNull(15 * 1000) {
                        deviceAddressSource.receive()
                    } ?: lastDeviceAddressList
                }
            }
        }
    }
}
