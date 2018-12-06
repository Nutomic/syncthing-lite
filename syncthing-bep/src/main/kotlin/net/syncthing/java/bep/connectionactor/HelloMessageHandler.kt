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

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.utils.NetworkUtils
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

object HelloMessageHandler {
    private val logger = LoggerFactory.getLogger(HelloMessageHandler::class.java)

    fun sendHelloMessage(configuration: Configuration, outputStream: DataOutputStream) {
        sendHelloMessage(
                BlockExchangeProtos.Hello.newBuilder()
                        .setClientName(configuration.clientName)
                        .setClientVersion(configuration.clientVersion)
                        .setDeviceName(configuration.localDeviceName)
                        .build(),
                outputStream
        )
    }

    private fun sendHelloMessage(message: BlockExchangeProtos.Hello, outputStream: DataOutputStream) {
        sendHelloMessage(message.toByteArray(), outputStream)
    }

    private fun sendHelloMessage(payload: ByteArray, outputStream: DataOutputStream) {
        logger.debug("Sending hello message")

        outputStream.apply {
            write(
                    ByteBuffer.allocate(6).apply {
                        putInt(ConnectionConstants.MAGIC)
                        putShort(payload.size.toShort())
                    }.array()
            )
            write(payload)
            flush()
        }
    }

    fun receiveHelloMessage(
            inputStream: DataInputStream
    ): BlockExchangeProtos.Hello {
        val magic = inputStream.readInt()
        NetworkUtils.assertProtocol(magic == ConnectionConstants.MAGIC) {"magic mismatch, got $magic"}

        val length = inputStream.readShort().toInt()
        NetworkUtils.assertProtocol(length > 0) {"invalid length, must be > 0, got $length"}

        return BlockExchangeProtos.Hello.parseFrom(
                ByteArray(length).apply {
                    inputStream.readFully(this)
                }
        )
    }

    suspend fun processHelloMessage(
            hello: BlockExchangeProtos.Hello,
            configuration: Configuration,
            deviceId: DeviceId
    ) {
        logger.info("Received hello message, deviceName=${hello.deviceName}, clientName=${hello.clientName}, clientVersion=${hello.clientVersion}")

        // update the local device name
        configuration.update { oldConfig ->
            oldConfig.copy(
                    peers = oldConfig.peers.map { peer ->
                        if (peer.deviceId == deviceId) {
                            DeviceInfo(deviceId, hello.deviceName)
                        } else {
                            peer
                        }
                    }.toSet()
            )
        }

        configuration.persistLater()
    }
}
