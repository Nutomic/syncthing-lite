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

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.discovery.utils.AddressRanker
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpGet
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContextBuilder
import org.apache.http.conn.ssl.TrustSelfSignedStrategy
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.io.StringReader
import java.security.KeyManagementException
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.util.*

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
        try {
            logger.debug("querying server {} for device id {}", server, deviceId)
            val httpClient = HttpClients.custom()
                    .setSSLSocketFactory(SSLConnectionSocketFactory(SSLContextBuilder().loadTrustMaterial(null, TrustSelfSignedStrategy()).build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER))
                    .build()
            val httpGet = HttpGet("https://$server/v2/?device=${deviceId.deviceId}")
            return httpClient.execute<List<DeviceAddress>>(httpGet) { response ->
                when (response.statusLine.statusCode) {
                    HttpStatus.SC_NOT_FOUND -> {
                        logger.debug("device not found: {}", deviceId)
                        return@execute emptyList()
                    }
                    HttpStatus.SC_OK -> {
                        val announcementMessage = AnnouncementMessage.parse(
                                JsonReader(
                                        StringReader(
                                                EntityUtils.toString(response.entity)
                                        )
                                )
                        )
                        return@execute (announcementMessage.addresses)
                                .map { DeviceAddress(deviceId.deviceId, it) }
                    }
                    else -> {
                        throw IOException("http error ${response.statusLine}, response ${EntityUtils.toString(response.entity)}")
                    }
                }
            }
        } catch (e: Exception) {
            when (e) {
                is IOException, is NoSuchAlgorithmException, is KeyStoreException, is KeyManagementException ->
                    throw IOException(e)
                else -> throw e
            }
        }
    }

    override fun close() {}

    private data class AnnouncementMessage(val addresses: List<String>) {
        companion object {
            private const val ADDRESSES = "addresses"

            fun parse(reader: JsonReader): AnnouncementMessage {
                var addresses = listOf<String>()

                reader.beginObject()
                while (reader.hasNext()) {
                    when (reader.nextName()) {
                        ADDRESSES -> {
                            val newAddresses = ArrayList<String>()

                            if (reader.peek() == JsonToken.NULL) {
                                reader.skipValue()
                            } else {
                                reader.beginArray()
                                while (reader.hasNext()) {
                                    newAddresses.add(reader.nextString())
                                }
                                reader.endArray()
                            }

                            addresses = Collections.unmodifiableList(newAddresses)
                        }
                        else -> reader.skipValue()
                    }
                }
                reader.endObject()

                return AnnouncementMessage(addresses)
            }
        }
    }
}
