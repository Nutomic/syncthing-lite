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

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.configuration.DiscoveryServer
import org.slf4j.LoggerFactory
import java.io.IOException

internal class GlobalDiscoveryHandler(private val configuration: Configuration) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Deprecated(message = "coroutine version should be used instead of callback")
    fun query(deviceId: DeviceId, callback: (List<DeviceAddress>) -> Unit) = GlobalScope.launch {
        try {
            callback(query(deviceId))
        } catch (ex: Exception) {
            callback(emptyList())
        }
    }

    suspend fun query(deviceIds: Collection<DeviceId>): List<DeviceAddress> {
        val discoveryServers = getLookupServers()

        return coroutineScope {
            deviceIds
                    .distinct()
                    .map { deviceId ->
                        async {
                            queryAnnounceServers(
                                    servers = discoveryServers,
                                    deviceId = deviceId
                            )
                        }
                    }
                    .map { it.await() }
                    .flatten()
        }
    }

    suspend fun query(deviceId: DeviceId) = queryAnnounceServers(
            servers = getLookupServers(),
            deviceId = deviceId
    )

    fun getLookupServers() = configuration.discoveryServers.filter { it.useForLookup }

    suspend fun queryAnnounceServers(servers: List<DiscoveryServer>, deviceId: DeviceId) = coroutineScope {
        servers
                .map { server ->
                    async {
                        try {
                            queryAnnounceServer(server, deviceId)
                        } catch (ex: Exception) {
                            logger.warn("Failed to query $server for $deviceId", ex)

                            when (ex) {
                                is IOException -> { /* ignore */ }
                                is DeviceNotFoundException -> { /* ignore */ }
                                is TooManyRequestsException -> { /* ignore */ }
                                else -> throw ex
                            }

                            emptyList<DeviceAddress>()
                        }
                    }
                }
                .map { it.await() }
                .flatten()
        // .distinct() is not required because the device addresses contain the used discovery server
    }

    companion object {
        suspend fun queryAnnounceServer(server: DiscoveryServer, deviceId: DeviceId) =
                GlobalDiscoveryUtil
                        .queryAnnounceServer(
                                server = server.hostname,
                                requestedDeviceId = deviceId,
                                serverDeviceId = server.deviceId
                        )
                        .addresses.map { DeviceAddress(deviceId.deviceId, it) }
    }
}
