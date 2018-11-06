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

        invokeOnClose { deviceAddress.cancel() }

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
        invokeOnClose { source.cancel() }

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

    private fun <T> debounce(source: ReceiveChannel<T>, time: Long) = GlobalScope.produce <T> {
        invokeOnClose { source.cancel() }

        source.consume {
            // first value without delay
            send(source.receive())

            // all other values only after the time and conflated
            while (true) {
                var lastValue: T? = null

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

                send(lastValue ?: source.receive())
            }
        }

        source.consumeEach {
            offer(it)
            delay(time)
        }
    }

    fun generateConnectionActors(
            deviceAddress: ReceiveChannel<DeviceAddress>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = generateConnectionActorsFromDeviceAddressList(
            deviceAddressSource = debounce(
                    source = waitForFirstValue(
                            source = deviceAddressesGenerator(deviceAddress),
                            time = 1000
                    ),
                    time = 5 * 1000
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

        invokeOnClose {
            deviceAddressSource.cancel()
            currentActor.close()
        }

        deviceAddressSource.consumeEach { deviceAddresses ->
            logger.debug("try $deviceAddresses")

            if (deviceAddresses.isEmpty()) {
                closeCurrent()
            } else {
                if (currentDeviceAddress == deviceAddresses.first() && (!currentActor.isClosedForSend)) {
                    // don't reconnect
                    return@consumeEach
                }

                for (deviceAddress in deviceAddresses) {
                    closeCurrent()

                    if (!deviceAddressSource.isEmpty) {
                        // time to consume the next value
                        break
                    }

                    val newActor = ConnectionActor.createInstance(deviceAddress, configuration, indexHandler, requestHandler)

                    val clusterConfig = try {
                        ConnectionActorUtil.waitUntilConnected(newActor)
                    } catch (ex: Exception) {
                        logger.warn("failed to connect to $deviceAddress", ex)

                        when (ex) {
                            is IOException -> {/* expected -> ignore */}
                            is InterruptedException -> {/* expected -> ignore */}
                            else -> throw ex
                        }

                        continue
                    }

                    logger.debug("connected to $deviceAddress")

                    currentActor = newActor
                    currentDeviceAddress = deviceAddress

                    send(newActor to clusterConfig)

                    break   // don't try the other addresses
                }
            }
        }
    }
}
