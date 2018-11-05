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
import java.io.IOException
import javax.net.ssl.SSLSocket

object OpenConnection {
    private val logger = LoggerFactory.getLogger(OpenConnection::class.java)

    fun openSocketConnection(
            address: DeviceAddress,
            configuration: Configuration
    ): SSLSocket {
        val keystoreHandler = KeystoreHandler.Loader().loadKeystore(configuration)

        return when (address.getType()) {
            DeviceAddress.AddressType.TCP -> {
                logger.debug("opening tcp ssl connection")
                keystoreHandler.createSocket(address.getSocketAddress(), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.RELAY -> {
                logger.debug("opening relay connection")
                keystoreHandler.wrapSocket(RelayClient(configuration).openRelayConnection(address), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.HTTP_RELAY, DeviceAddress.AddressType.HTTPS_RELAY -> {
                logger.debug("opening http relay connection")

                throw IOException("http relay support is disabled")
                // This is not documented anywhere (https://forum.syncthing.net/t/http-relays/12367) so it's disabled
                // it's still kept because it is eventually useful in the future
                // keystoreHandler.wrapSocket(HttpRelayClient().openRelayConnection(address), KeystoreHandler.BEP)
            }
            else -> throw UnsupportedOperationException("unsupported address type ${address.getType()}")
        }
    }
}
