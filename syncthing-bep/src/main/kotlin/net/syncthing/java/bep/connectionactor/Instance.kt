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
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.util.*

object ConnectionActor {
    fun createInstance(
            address: DeviceAddress,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ): SendChannel<ConnectionAction> {
        val channel = Channel<ConnectionAction>(Channel.RENDEZVOUS)

        GlobalScope.async (Dispatchers.IO) {
            OpenConnection.openSocketConnection(address, configuration).use { socket ->
                val inputStream = DataInputStream(socket.inputStream)
                val outputStream = DataOutputStream(socket.outputStream)

                val helloMessage = coroutineScope {
                    async { HelloMessageHandler.sendHelloMessage(configuration, outputStream) }
                    async { HelloMessageHandler.receiveHelloMessage(inputStream) }.await()
                }

                // the hello message exchange should happen before the certificate validation
                KeystoreHandler.assertSocketCertificateValid(socket, address.deviceId)

                // now (after the validation) use the content of the hello message
                HelloMessageHandler.processHelloMessage(helloMessage, configuration, address.deviceId)

                // helpers for messages
                val sendPostAuthMessageLock = Mutex()
                val receivePostAuthMessageLock = Mutex()

                suspend fun sendPostAuthMessage(message: MessageLite) = sendPostAuthMessageLock.withLock {
                    PostAuthenticationMessageHandler.sendMessage(outputStream, message, markActivityOnSocket = {})
                }

                suspend fun receivePostAuthMessage() = receivePostAuthMessageLock.withLock {
                    PostAuthenticationMessageHandler.receiveMessage(inputStream, markActivityOnSocket = {})
                }

                // cluster config exchange
                val clusterConfig = coroutineScope {
                    launch { sendPostAuthMessage(ClusterConfigHandler.buildClusterConfig(configuration, indexHandler, address.deviceId)) }
                    async { receivePostAuthMessage() }.await()
                }.second

                if (!(clusterConfig is BlockExchangeProtos.ClusterConfig)) {
                    throw IOException("first message was not a cluster config message")
                }

                val clusterConfigInfo = ClusterConfigHandler.handleReceivedClusterConfig(
                        clusterConfig = clusterConfig,
                        configuration = configuration,
                        otherDeviceId = address.deviceId,
                        indexHandler = indexHandler
                )

                fun hasFolder(folder: String) = clusterConfigInfo.sharedFolderIds.contains(folder)

                val messageListeners = Collections.synchronizedMap(mutableMapOf<Int, CompletableDeferred<BlockExchangeProtos.Response>>())

                try {
                    launch {
                        while (isActive) {
                            val message = receivePostAuthMessage().second

                            when (message) {
                                is BlockExchangeProtos.Response -> {
                                    val listener = messageListeners.remove(message.id)
                                    listener
                                            ?: throw IOException("got response ${message.id} but there is no response listener")
                                    listener.complete(message)
                                }
                                is BlockExchangeProtos.Index -> {
                                    indexHandler.handleIndexMessageReceivedEvent(
                                            folderId = message.folder,
                                            filesList = message.filesList,
                                            clusterConfigInfo = clusterConfigInfo,
                                            peerDeviceId = address.deviceId
                                    )
                                }
                                is BlockExchangeProtos.IndexUpdate -> {
                                    indexHandler.handleIndexMessageReceivedEvent(
                                            folderId = message.folder,
                                            filesList = message.filesList,
                                            clusterConfigInfo = clusterConfigInfo,
                                            peerDeviceId = address.deviceId
                                    )
                                }
                                is BlockExchangeProtos.Request -> {
                                    launch {
                                        val response = requestHandler(message).await()

                                        try {
                                            sendPostAuthMessage(response)
                                        } catch (ex: IOException) {
                                            // the connection was closed in the time between - ignore it
                                        }
                                    }
                                }
                                is BlockExchangeProtos.Ping -> { /* nothing to do */
                                }
                                is BlockExchangeProtos.ClusterConfig -> throw IOException("received cluster config twice")
                                is BlockExchangeProtos.Close -> socket.close()
                                else -> throw IOException("unsupported message type ${message.javaClass}")
                            }
                        }
                    }

                    // send index messages - TODO: Why?
                    for (folder in configuration.folders) {
                        if (hasFolder(folder.folderId)) {
                            sendPostAuthMessage(
                                    BlockExchangeProtos.Index.newBuilder()
                                            .setFolder(folder.folderId)
                                            .build()
                            )
                        }
                    }

                    launch {
                        // send ping all 90 seconds
                        // TODO: only send when there were no messages for 90 seconds

                        while (isActive) {
                            delay(90 * 1000)

                            launch { sendPostAuthMessage(BlockExchangeProtos.Ping.getDefaultInstance()) }
                        }
                    }

                    var nextRequestId = 0

                    channel.consumeEach { action ->
                        when (action) {
                            CloseConnectionAction -> throw InterruptedException()
                            is SendRequestConnectionAction -> {
                                val requestId = nextRequestId++

                                messageListeners[requestId] = action.completableDeferred

                                // async to allow handling the next action faster
                                async {
                                    try {
                                        sendPostAuthMessage(
                                                action.request.toBuilder()
                                                        .setId(requestId)
                                                        .build()
                                        )
                                    } catch (ex: Exception) {
                                        action.completableDeferred.cancel(ex)
                                    }
                                }
                            }
                            is ConfirmIsConnectedAction -> {
                                action.completableDeferred.complete(clusterConfigInfo)

                                // otherwise, Kotlin would warn that the return
                                // type does not match to the other branches
                                null
                            }
                            is SendIndexUpdateAction -> {
                                async {
                                    try {
                                        sendPostAuthMessage(action.message)
                                    } catch (ex: Exception) {
                                        action.completableDeferred.cancel(ex)
                                    }
                                }
                            }
                        }.let { /* prevents compiling if one action is not handled */ }
                    }
                } finally {
                    // send close message
                    withContext(NonCancellable) {
                        if (socket.isConnected) {
                            sendPostAuthMessage(BlockExchangeProtos.Close.getDefaultInstance())
                        }
                    }

                    // cancel all pending listeners
                    messageListeners.values.forEach { it.cancel() }
                }
            }
        }.invokeOnCompletion { ex ->
            if (ex != null) {
                channel.cancel(ex)
            } else {
                channel.cancel()
            }
        }

        return channel
    }
}
