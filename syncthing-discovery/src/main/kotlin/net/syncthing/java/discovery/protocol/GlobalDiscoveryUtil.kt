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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.security.KeystoreHandler
import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager

object GlobalDiscoveryUtil {
    private fun queryAnnounceServerUrl(server: String, deviceId: DeviceId) =
            "https://$server/v2/?device=${deviceId.deviceId}"

    suspend fun queryAnnounceServer(
            server: String,
            requestedDeviceId: DeviceId,
            serverDeviceId: DeviceId?
    ): AnnouncementMessage {
        return withContext(Dispatchers.IO) {
            val url = URL(queryAnnounceServerUrl(server, requestedDeviceId))
            val connection = (url.openConnection() as HttpsURLConnection).apply {
                hostnameVerifier = HostnameVerifier { _, session ->
                    try {
                        if (serverDeviceId != null) {
                            if (session.peerCertificates.isEmpty()) {
                                throw IOException("no certificate found")
                            }

                            KeystoreHandler.assertSocketCertificateValid(session.peerCertificates.first(), serverDeviceId)
                        }

                        true
                    } catch (ex: Exception) {
                        when (ex) {
                            is IOException -> false
                            is CertificateException -> false
                            else -> throw ex
                        }
                    }
                }
                sslSocketFactory = SSLContext.getInstance("SSL").apply {
                    init(null, arrayOf(object: X509TrustManager {
                        override fun checkClientTrusted(p0: Array<out X509Certificate>?, p1: String?) {
                            // check nothing
                        }

                        override fun checkServerTrusted(p0: Array<out X509Certificate>?, p1: String?) {
                            // check nothing
                        }

                        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
                    }), SecureRandom())
                }.socketFactory
            }

            try {
                connection.connect()

                when (connection.responseCode) {
                    HttpURLConnection.HTTP_NOT_FOUND -> throw DeviceNotFoundException()
                    429 -> throw TooManyRequestsException()
                    HttpURLConnection.HTTP_OK -> {
                        JsonReader(InputStreamReader(BufferedInputStream(connection.inputStream))).use { reader ->
                            AnnouncementMessage.parse(reader)
                        }
                    }
                    else -> throw IOException("http error ${connection.responseCode}: ${connection.responseMessage}")
                }
            } finally {
                connection.disconnect()
            }
        }
    }
}

class DeviceNotFoundException: RuntimeException()
class TooManyRequestsException: RuntimeException()
// TODO: handle too many requests -> stop sending requests for some time
