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
package net.syncthing.java.httprelay

import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import org.slf4j.LoggerFactory

class HttpRelayClient {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun openRelayConnection(deviceAddress: DeviceAddress): HttpRelayConnection {
        assert(setOf(AddressType.HTTP_RELAY, AddressType.HTTPS_RELAY).contains(deviceAddress.getType()))
        val httpRelayServerUrl = deviceAddress.address.replaceFirst("^relay-".toRegex(), "")
        val deviceId = deviceAddress.deviceId
        logger.info("open http relay connection, relay url = {}, target device id = {}", httpRelayServerUrl, deviceId)
        return HttpRelayConnection(httpRelayServerUrl, deviceId.deviceId)
    }
}
