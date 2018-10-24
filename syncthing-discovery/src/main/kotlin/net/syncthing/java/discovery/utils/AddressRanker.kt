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
package net.syncthing.java.discovery.utils

import kotlinx.coroutines.experimental.*
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.Socket

object AddressRanker {

    private const val TCP_CONNECTION_TIMEOUT = 5000
    private val BASE_SCORE_MAP = mapOf(
            AddressType.TCP to 0,
            AddressType.RELAY to 2000,
            AddressType.HTTP_RELAY to 1000 * 2000,
            AddressType.HTTPS_RELAY to 1000 * 2000
    )
    private val ACCEPTED_ADDRESS_TYPES = BASE_SCORE_MAP.keys
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun pingAddresses(sourceAddresses: List<DeviceAddress>) = coroutineScope {
        addHttpRelays(sourceAddresses)
                .filter { ACCEPTED_ADDRESS_TYPES.contains(it.getType()) }
                .toList()   // the following should happen parallel
                .map {
                    async {
                        try {
                            withTimeout(TCP_CONNECTION_TIMEOUT * 2L) {
                                // this nested async ensures that cancelling/ the timeout has got an effect without delay
                                GlobalScope.async (Dispatchers.IO) {
                                    pingAddressSync(it)
                                }.await()
                            }
                        } catch (ex: Exception) {
                            logger.warn("Failed to ping device", ex)

                            null
                        }
                    }
                }
                .map { it.await() }
                .filterNotNull()
                .sortedBy { it.score }
    }

    private fun getHttpRelays(list: List<DeviceAddress>) = list
            .asSequence()
            .filter { address ->
                address.getType() == AddressType.RELAY && address.containsUriParamValue("httpUrl")
            }
            .map { address ->
                val httpUrl = address.getUriParam("httpUrl")
                address.copyBuilder().setAddress("relay-" + httpUrl!!).build()
            }

    private fun addHttpRelays(list: List<DeviceAddress>) = getHttpRelays(list) + list

    private fun pingAddressSync(deviceAddress: DeviceAddress): DeviceAddress? {
        val startTime = System.currentTimeMillis()

        try {
            Socket().use { socket ->
                socket.soTimeout = TCP_CONNECTION_TIMEOUT
                socket.connect(deviceAddress.getSocketAddress(), TCP_CONNECTION_TIMEOUT)
            }
        } catch (ex: IOException) {
            logger.debug("address unreacheable = $deviceAddress, ${ex.message}")
            return null
        }

        val ping = (System.currentTimeMillis() - startTime).toInt()
        val baseScore = BASE_SCORE_MAP[deviceAddress.getType()] ?: 0

        return deviceAddress.copyBuilder().setScore(ping + baseScore).build()
    }
}
