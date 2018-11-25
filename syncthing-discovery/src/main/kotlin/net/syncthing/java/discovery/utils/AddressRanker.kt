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

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.toList
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.Socket

object AddressRanker {

    private const val TCP_CONNECTION_TIMEOUT = 5000
    private val BASE_SCORE_MAP = mapOf(
            AddressType.TCP to 0,
            AddressType.RELAY to 2000
    )
    private val ACCEPTED_ADDRESS_TYPES = BASE_SCORE_MAP.keys
    private val logger = LoggerFactory.getLogger(javaClass)

    fun pingAddressesChannel(sourceAddresses: List<DeviceAddress>) = GlobalScope.produce<DeviceAddress> {
        sourceAddresses
                .filter { ACCEPTED_ADDRESS_TYPES.contains(it.type) }
                .toList()
                .map { address ->
                    async {
                        try {
                            val addressWithScore = withTimeout(TCP_CONNECTION_TIMEOUT * 2L) {
                                // this nested async ensures that cancelling/ the timeout has got an effect without delay
                                GlobalScope.async (Dispatchers.IO) {
                                    pingAddressSync(address)
                                }.await()
                            }

                            if (addressWithScore != null) {
                                send(addressWithScore)
                            }
                        } catch (ex: Exception) {
                            logger.warn("Failed to ping device", ex)
                        }

                        null
                    }
                }
                .map { it.await() }

        close()
    }

    @Deprecated(
            message = "This is slower than the version which returns the channel",
            replaceWith = ReplaceWith("pingAddressesChannel")
    )
    suspend fun pingAddressesReturnAllResultsAtOnce(sourceAddresses: List<DeviceAddress>) = pingAddressesChannel(sourceAddresses)
            .toList()
            .sortedBy { it.score }

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
        val baseScore = BASE_SCORE_MAP[deviceAddress.type] ?: 0

        return deviceAddress.copyBuilder().setScore(ping + baseScore).build()
    }
}
