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

import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.delay
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration

object ConnectionActorGenerator {
    private val closed = Channel<ConnectionAction>().apply { cancel() }

    fun deviceAddressesGenerator(deviceAddress: ReceiveChannel<DeviceAddress>) = GlobalScope.produce<List<DeviceAddress>> (capacity = Channel.CONFLATED) {
        val addresses = mutableMapOf<String, DeviceAddress>()

        invokeOnClose { deviceAddress.cancel() }

        deviceAddress.consumeEach { address ->
            val isNew = addresses[address.address] == null

            addresses[address.address] = address

            if (isNew) {
                offer(
                        addresses.values.sortedBy { it.score }
                )
            }
        }
    }

    fun <T> waitForFirstValue(source: ReceiveChannel<T>, time: Long) = GlobalScope.produce<T> {
        invokeOnClose { source.cancel() }

        delay(time)

        source.consumeEach {
            offer(it)
        }
    }

    fun <T> debounce(source: ReceiveChannel<T>, time: Long) = GlobalScope.produce <T> {
        invokeOnClose { source.cancel() }

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
    ) = generateConnectionActors2(
            deviceAddressSource = debounce(
                    source = waitForFirstValue(
                            source = deviceAddressesGenerator(deviceAddress),
                            time = 1000
                    ),
                    time = 15 * 1000
            ),
            configuration = configuration,
            indexHandler = indexHandler,
            requestHandler = requestHandler
    )


    fun generateConnectionActors2(
            deviceAddressSource: ReceiveChannel<List<DeviceAddress>>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = GlobalScope.produce {
        var currentActor: SendChannel<ConnectionAction> = closed
        var currentDeviceAddress: DeviceAddress? = null

        suspend fun closeCurrent() {
            if (currentActor != closed) {
                currentActor.close()
                currentActor = closed
                send(currentActor)
            }
        }

        invokeOnClose {
            deviceAddressSource.cancel()
            currentActor.close()
        }

        deviceAddressSource.consumeEach { deviceAddresses ->
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

                    try {
                        ConnectionActorUtil.waitUntilConnected(newActor)
                    } catch (ex: Exception) {
                        // TODO: catch more specific
                        // TODO: log exception?

                        continue
                    }

                    currentActor = newActor
                    currentDeviceAddress = deviceAddress

                    send(newActor)

                    break   // don't try the other addresses
                }
            }
        }
    }
}
