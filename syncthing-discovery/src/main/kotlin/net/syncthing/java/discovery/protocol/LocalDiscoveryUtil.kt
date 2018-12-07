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

import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.exception.ExceptionDetailException
import net.syncthing.java.core.exception.ExceptionDetails
import net.syncthing.java.core.utils.NetworkUtils
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import java.lang.Exception
import java.net.*
import java.nio.ByteBuffer

object LocalDiscoveryUtil {
    private const val LISTENING_PORT = 21027
    private const val MAGIC = 0x2EA7D90B
    private const val INCOMING_BUFFER_SIZE = 1024

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun listenForAnnounceMessages(): ReceiveChannel<LocalDiscoveryMessage> = GlobalScope.produce {
        DatagramSocket(LISTENING_PORT, InetAddress.getByName("0.0.0.0")).use { datagramSocket ->
            invokeOnClose {
                datagramSocket.close()
            }

            withContext(Dispatchers.IO) {
                val datagramPacket = DatagramPacket(ByteArray(INCOMING_BUFFER_SIZE), INCOMING_BUFFER_SIZE)

                while (!isClosedForSend) {
                    try {
                        datagramSocket.receive(datagramPacket)
                    } catch (ex: SocketException) {
                        if (datagramSocket.isClosed) {
                            // if the socket was closed by the invokeOnClose, then ignore it

                            return@withContext
                        } else {
                            // otherwise it's more serious and it is rethrown

                            throw ex
                        }
                    }

                    try {
                        val sourceAddress = datagramPacket.address.hostAddress
                        val byteBuffer = ByteBuffer.wrap(datagramPacket.data, datagramPacket.offset, datagramPacket.length)
                        val magic = byteBuffer.int
                        NetworkUtils.assertProtocol(magic == MAGIC) {
                            "magic mismatch, expected ${MAGIC}, got $magic"
                        }
                        val announce = LocalDiscoveryProtos.Announce.parseFrom(ByteString.copyFrom(byteBuffer))
                        val deviceId = DeviceId.fromHashData(announce.id.toByteArray())

                        val deviceAddresses = (announce.addressesList ?: emptyList()).map { address ->
                            // When interpreting addresses with an unspecified address, e.g.,
                            // tcp://0.0.0.0:22000 or tcp://:42424, the source address of the
                            // discovery announcement is to be used.
                            DeviceAddress.Builder()
                                    .setAddress(address.replaceFirst("tcp://(0.0.0.0|):".toRegex(), "tcp://$sourceAddress:"))
                                    .setDeviceId(deviceId)
                                    .setInstanceId(announce.instanceId)
                                    .setProducer(DeviceAddress.AddressProducer.LOCAL_DISCOVERY)
                                    .build()
                        }

                        val message = LocalDiscoveryMessage(
                                deviceId = deviceId,
                                addresses = deviceAddresses
                        )

                        send(message)
                    } catch (ex: IOException) {
                        logger.warn("error during handling received package", ex)
                    }
                }
            }
        }
    }

    fun sendAnnounceMessage(ownDeviceId: DeviceId, instanceId: Long) {
        val discoveryMessage = ByteArrayOutputStream().apply {
            DataOutputStream(this).writeInt(MAGIC)

            LocalDiscoveryProtos.Announce.newBuilder()
                    .setId(ByteString.copyFrom(ownDeviceId.toHashData()))
                    .setInstanceId(instanceId)
                    .build()
                    .writeTo(this)
        }.toByteArray()

        for (networkInterface in NetworkInterface.getNetworkInterfaces()) {
            for (interfaceAddress in networkInterface.interfaceAddresses) {
                val broadcastAddress = interfaceAddress.broadcast

                logger.trace("interface = {} address = {} broadcast = {}", networkInterface, interfaceAddress, broadcastAddress)

                if (broadcastAddress != null) {
                    logger.debug("sending broadcast announce on {}", broadcastAddress)

                    try {
                        DatagramSocket().use { broadcastSocket ->
                            broadcastSocket.broadcast = true

                            broadcastSocket.send(DatagramPacket(
                                    discoveryMessage,
                                    discoveryMessage.size,
                                    broadcastAddress,
                                    LISTENING_PORT))
                        }
                    } catch (ex: Exception) {
                        throw ExceptionDetailException(
                                ex,
                                ExceptionDetails(
                                        component = "LocalDiscoveryUtil.sendAnnounceMessage",
                                        details = "interface: $networkInterface\naddress: $interfaceAddress\nbroadcast address: $broadcastAddress"
                                )
                        )
                    }
                }
            }
        }
    }
}

data class LocalDiscoveryMessage(val deviceId: DeviceId, val addresses: List<DeviceAddress>) {
    init {
        addresses.forEach { address ->
            if (address.deviceId != deviceId) {
                throw IllegalArgumentException()
            }
        }
    }
}
