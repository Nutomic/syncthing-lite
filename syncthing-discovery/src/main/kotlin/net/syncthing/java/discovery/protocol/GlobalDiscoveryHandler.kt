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
package net.syncthing.java.discovery.protocol

import kotlinx.coroutines.experimental.runBlocking
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.discovery.utils.AddressRanker
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException

internal class GlobalDiscoveryHandler(private val configuration: Configuration) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun query(deviceId: DeviceId, callback: (List<DeviceAddress>) -> Unit) {
        val addresses = pickAnnounceServers()
                .map {
                    try {
                        queryAnnounceServer(it, deviceId)
                    } catch (e: IOException) {
                        logger.warn("Failed to query $it", e)
                        listOf<DeviceAddress>()
                    }
                }
                .flatten()
        callback(addresses)
    }

    private fun pickAnnounceServers(): List<String> {
        val list = AddressRanker
                .pingAddresses(configuration.discoveryServers.map { DeviceAddress(it, "tcp://$it:443") })
        return list.map { it.deviceId }
    }

    @Throws(IOException::class)
    private fun queryAnnounceServer(server: String, deviceId: DeviceId): List<DeviceAddress> {
        logger.debug("querying server {} for device id {}", server, deviceId)

        return runBlocking {
            GlobalDiscoveryUtil.queryAnnounceServer(server, deviceId.deviceId).addresses.map {
                DeviceAddress(deviceId.deviceId, it)
            }
        }
    }

    override fun close() {}
}
