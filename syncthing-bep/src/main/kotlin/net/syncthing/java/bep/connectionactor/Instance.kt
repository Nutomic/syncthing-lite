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

import com.google.protobuf.MessageLite
import kotlinx.coroutines.experimental.Dispatchers
import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.actor
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.coroutineScope
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import java.io.DataInputStream
import java.io.DataOutputStream

object ConnectionActor {
    fun createInstance(
            address: DeviceAddress,
            configuration: Configuration,
            indexHandler: IndexHandler
    ) {
        GlobalScope.actor<ConnectionAction>(Dispatchers.IO) {
            OpenConnection.openSocketConnection(address, configuration).use { socket ->
                val inputStream = DataInputStream(socket.inputStream)
                val outputStream = DataOutputStream(socket.outputStream)

                val helloMessage = coroutineScope {
                    async { HelloMessageHandler.sendHelloMessage(configuration, outputStream) }
                    async { HelloMessageHandler.receiveHelloMessage(inputStream) }.await()
                }

                // the hello message exchange should happen before the certificate validation
                KeystoreHandler.assertSocketCertificateValid(socket, address.deviceIdObject)

                // now (after the validation) use the content of the hello message
                HelloMessageHandler.processHelloMessage(helloMessage, configuration, address.deviceIdObject)

                // helper to send messages
                val sendPostAuthMessageLock = Mutex()

                suspend fun sendPostAuthMessage(message: MessageLite) {
                    sendPostAuthMessageLock.withLock {
                        PostAuthenticationMessageHandler.sendMessage(outputStream, message, markActivityOnSocket = {})
                    }
                }

                // cluster config exchange
                sendPostAuthMessage(ClusterConfigHandler.buildClusterConfig(configuration, indexHandler, address.deviceIdObject))

                // TODO: receive cluster config
                // TODO: index message exchange

                consumeEach { action ->
                    TODO()
                }
            }
        }
    }
}
