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
package net.syncthing.java.discovery

import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.core.utils.submitLogging
import net.syncthing.java.discovery.protocol.GlobalDiscoveryHandler
import net.syncthing.java.discovery.protocol.LocalDiscoveryHandler
import net.syncthing.java.discovery.utils.AddressRanker
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*
import java.util.concurrent.Executors

class DiscoveryHandler(private val configuration: Configuration) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val globalDiscoveryHandler = GlobalDiscoveryHandler(configuration)
    private val localDiscoveryHandler = LocalDiscoveryHandler(configuration, { _, deviceAddresses ->
        logger.info("received device address list from local discovery")
        processDeviceAddressBg(deviceAddresses)
    }, { deviceId ->
        onMessageFromUnknownDeviceListeners.forEach { listener -> listener(deviceId) }
    })
    private val executorService = Executors.newCachedThreadPool()
    private val deviceAddressMap = Collections.synchronizedMap(hashMapOf<Pair<DeviceId, String>, DeviceAddress>())
    private val deviceAddressSupplier = DeviceAddressSupplier(this)
    private var isClosed = false
    private val onMessageFromUnknownDeviceListeners = Collections.synchronizedSet(HashSet<(DeviceId) -> Unit>())

    private var shouldLoadFromGlobal = true
    private var shouldStartLocalDiscovery = true

    fun getAllWorkingDeviceAddresses() = deviceAddressMap.values.filter { it.isWorking() }

    private fun updateAddressesBg() {
        if (shouldStartLocalDiscovery) {
            shouldStartLocalDiscovery = false
            localDiscoveryHandler.startListener()
            localDiscoveryHandler.sendAnnounceMessage()
        }
        if (shouldLoadFromGlobal) {
            shouldLoadFromGlobal = false //TODO timeout for reload
            executorService.submitLogging {
                for (deviceId in configuration.peerIds) {
                    globalDiscoveryHandler.query(deviceId, this::processDeviceAddressBg)
                }
            }
        }
    }

    private fun processDeviceAddressBg(deviceAddresses: Iterable<DeviceAddress>) {
        if (isClosed) {
            logger.debug("discarding device addresses, discovery handler already closed")
        } else {
            executorService.submitLogging {
                val list = deviceAddresses.toList()
                val peers = configuration.peerIds
                //do not process address already processed
                list.filter { deviceAddress ->
                    !peers.contains(deviceAddress.deviceId()) || deviceAddressMap.containsKey(Pair.of(DeviceId(deviceAddress.deviceId), deviceAddress.address))
                }
                AddressRanker.pingAddresses(list)
                        .forEach { putDeviceAddress(it) }
            }
        }
    }

    private fun putDeviceAddress(deviceAddress: DeviceAddress) {
        deviceAddressMap[Pair.of(DeviceId(deviceAddress.deviceId), deviceAddress.address)] = deviceAddress
        deviceAddressSupplier.onNewDeviceAddressAcquired(deviceAddress)
    }

    fun newDeviceAddressSupplier(): DeviceAddressSupplier {
        updateAddressesBg()
        return deviceAddressSupplier
    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            localDiscoveryHandler.close()
            globalDiscoveryHandler.close()
            executorService.shutdown()
            executorService.awaitTerminationSafe()
        }
    }

    fun registerMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        onMessageFromUnknownDeviceListeners.add(listener)
    }

    fun unregisterMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        onMessageFromUnknownDeviceListeners.remove(listener)
    }
}
