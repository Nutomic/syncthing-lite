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
package net.syncthing.java.discovery.utils

import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import net.syncthing.java.core.utils.submitLogging
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.net.Socket
import java.util.concurrent.*

internal class AddressRanker private constructor(private val sourceAddresses: List<DeviceAddress>) : Closeable {

    companion object {

        private const val TCP_CONNECTION_TIMEOUT = 5000
        private val BASE_SCORE_MAP = mapOf(
                AddressType.TCP to 0,
                AddressType.RELAY to 2000,
                AddressType.HTTP_RELAY to 1000 * 2000,
                AddressType.HTTPS_RELAY to 1000 * 2000)
        private val ACCEPTED_ADDRESS_TYPES = BASE_SCORE_MAP.keys

        fun pingAddresses(list: List<DeviceAddress>): List<DeviceAddress> {
            AddressRanker(list).use { addressRanker ->
                return addressRanker.testAndRankAndWait()
            }
        }
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val executorService = Executors.newCachedThreadPool()

    private fun addHttpRelays(list: List<DeviceAddress>): List<DeviceAddress> {
        val httpRelays = list
                .filter { address ->
                    address.getType() == AddressType.RELAY && address.containsUriParamValue("httpUrl")
                }
                .map { address ->
                    val httpUrl = address.getUriParam("httpUrl")
                    address.copyBuilder().setAddress("relay-" + httpUrl!!).build()
                }
        return httpRelays + list
    }

    private fun testAndRankAndWait(): List<DeviceAddress> {
        return addHttpRelays(sourceAddresses)
                .filter { ACCEPTED_ADDRESS_TYPES.contains(it.getType()) }
                .map { executorService.submitLogging<DeviceAddress?> { pingAddresses(it) } }
                .mapNotNull { future ->
                    try {
                        future.get((TCP_CONNECTION_TIMEOUT * 2).toLong(), TimeUnit.MILLISECONDS)
                    } catch (e: ExecutionException) {
                        logger.warn("Failed to ping device", e)
                        null
                    } catch (e: InterruptedException) {
                        logger.warn("Failed to ping device", e)
                        null
                    }
                }
                .sortedBy { it.score }
    }

    private fun pingAddresses(deviceAddress: DeviceAddress): DeviceAddress? {
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

    override fun close() {
        executorService.shutdown()
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            logger.warn("", ex)
        }
    }
}
