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
package net.syncthing.java.bep.connectionactor

import net.syncthing.java.client.protocol.rp.RelayClient
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import org.slf4j.LoggerFactory
import javax.net.ssl.SSLSocket

object OpenConnection {
    private val logger = LoggerFactory.getLogger(OpenConnection::class.java)

    fun openSocketConnection(
            address: DeviceAddress,
            configuration: Configuration
    ): SSLSocket {
        val keystoreHandler = KeystoreHandler.Loader().loadKeystore(configuration)

        return when (address.type) {
            DeviceAddress.AddressType.TCP -> {
                logger.debug("opening tcp ssl connection")
                keystoreHandler.createSocket(address.getSocketAddress())
            }
            DeviceAddress.AddressType.RELAY -> {
                logger.debug("opening relay connection")
                keystoreHandler.wrapSocket(RelayClient(configuration).openRelayConnection(address))
            }
            else -> throw UnsupportedOperationException("unsupported address type ${address.type}")
        }
    }
}
