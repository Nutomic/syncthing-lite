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

import kotlinx.coroutines.experimental.Dispatchers
import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.channels.actor
import kotlinx.coroutines.experimental.channels.consumeEach
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import java.io.DataInputStream
import java.io.DataOutputStream

object ConnectionActor {
    fun createInstance(
            address: DeviceAddress,
            configuration: Configuration
    ) {
        GlobalScope.actor<ConnectionAction>(Dispatchers.IO) {
            OpenConnection.openSocketConnection(address, configuration).use { socket ->
                val inputStream = DataInputStream(socket.inputStream)
                val outputStream = DataOutputStream(socket.outputStream)

                HelloMessageHandler.sendHelloMessage(configuration, outputStream)
                val helloMessage = HelloMessageHandler.receiveHelloMessage(inputStream)
                // TODO: hello messages are exchanged parallel
                // TODO: handle hello message content
                // TODO: check socket certificate - this happens after the hello message exchange
                // TODO: cluster config exchange
                // TODO: index message exchange

                consumeEach { action ->
                    TODO()
                }
            }
        }
    }
}
