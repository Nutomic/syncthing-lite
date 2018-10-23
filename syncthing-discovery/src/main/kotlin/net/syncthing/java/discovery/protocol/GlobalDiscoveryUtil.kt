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
import kotlinx.coroutines.experimental.Dispatchers
import kotlinx.coroutines.experimental.withContext
import net.syncthing.java.core.beans.DeviceId
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import java.io.IOException
import java.io.StringReader

object GlobalDiscoveryUtil {
    suspend fun queryAnnounceServer(server: String, deviceId: DeviceId): AnnouncementMessage {
        return withContext(Dispatchers.IO) {
            val httpGet = HttpGet("https://$server/v2/?device=${deviceId.deviceId}")

            HttpClients.createDefault().execute<AnnouncementMessage>(httpGet) { response ->
                when (response.statusLine.statusCode) {
                    HttpStatus.SC_OK -> {
                        AnnouncementMessage.parse(
                                JsonReader(
                                        StringReader(
                                                EntityUtils.toString(response.entity)
                                        )
                                )
                        )
                    }
                    HttpStatus.SC_NOT_FOUND -> throw DeviceNotFoundException()
                    429 -> throw TooManyRequestsException()
                    else -> throw IOException("http error ${response.statusLine}, response ${EntityUtils.toString(response.entity)}")
                }
            }
        }
    }
}

class DeviceNotFoundException: RuntimeException()
class TooManyRequestsException: RuntimeException()
