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
package net.syncthing.java.bep

import com.google.protobuf.MessageLite
import net.syncthing.java.bep.BlockExchangeProtos.*
import net.syncthing.java.bep.connectionactor.*
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.submitLogging
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.security.cert.CertificateException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLSocket

class ConnectionHandler(private val configuration: Configuration, val address: DeviceAddress,
                        private val indexHandler: IndexHandler,
                        private val tempRepository: TempRepository,
                        private val onNewFolderSharedListener: (ConnectionHandler, FolderInfo) -> Unit,
                        private val onConnectionChangedListener: (ConnectionHandler) -> Unit) : Closeable {

    private val outExecutorService = Executors.newSingleThreadExecutor()
    private val inExecutorService = Executors.newSingleThreadExecutor()
    private val messageProcessingService = Executors.newCachedThreadPool()
    private val periodicExecutorService = Executors.newSingleThreadScheduledExecutor()
    private lateinit var socket: SSLSocket
    private var inputStream: DataInputStream? = null
    private var outputStream: DataOutputStream? = null
    private var lastActive = Long.MIN_VALUE
    internal var clusterConfigInfo: ClusterConfigInfo? = null
        private set
    private val clusterConfigWaitingLock = Object()
    private val responseHandler = ResponseHandler()
    private val blockPuller = BlockPuller(this, indexHandler, responseHandler, tempRepository)
    private val blockPusher = BlockPusher(configuration.localDeviceId, this, indexHandler)
    private val onRequestMessageReceivedListeners = mutableSetOf<(Request) -> Unit>()
    private var isClosed = false
    var isConnected = false
        private set

    fun deviceId(): DeviceId = address.deviceId()

    private fun checkNotClosed() {
        NetworkUtils.assertProtocol(!isClosed, {"connection $this closed"})
    }

    internal fun registerOnRequestMessageReceivedListeners(listener: (Request) -> Unit) {
        onRequestMessageReceivedListeners.add(listener)
    }

    internal fun unregisterOnRequestMessageReceivedListeners(listener: (Request) -> Unit) {
        assert(onRequestMessageReceivedListeners.contains(listener))
        onRequestMessageReceivedListeners.remove(listener)
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    fun connect(): ConnectionHandler {
        checkNotClosed()
        assert(!isConnected, {"already connected!"})
        logger.info("connecting to {}", address.address)

        val socket = OpenConnection.openSocketConnection(
                address = address,
                configuration = configuration
        )

        inputStream = DataInputStream(socket.inputStream)
        outputStream = DataOutputStream(socket.outputStream)

        sendHelloMessageInBackground()
        markActivityOnSocket()

        receiveHelloMessage()
        try {
            KeystoreHandler.assertSocketCertificateValid(socket, address.deviceIdObject)
        } catch (e: CertificateException) {
            throw IOException(e)
        }

        sendMessage(ClusterConfigHandler.buildClusterConfig(configuration, indexHandler, deviceId()))

        synchronized(clusterConfigWaitingLock) {
            startMessageListenerService()
            while (clusterConfigInfo == null && !isClosed) {
                logger.debug("wait for cluster config")
                try {
                    clusterConfigWaitingLock.wait()
                } catch (e: InterruptedException) {
                    throw IOException(e)
                }
            }
            if (clusterConfigInfo == null) {
                throw IOException("unable to retrieve cluster config from peer!")
            }
        }
        for (folder in configuration.folders) {
            if (hasFolder(folder.folderId)) {
                sendIndexMessage(folder.folderId)
            }
        }
        periodicExecutorService.scheduleWithFixedDelay({ this.sendPing() }, 90, 90, TimeUnit.SECONDS)
        isConnected = true
        onConnectionChangedListener(this)
        return this
    }

    fun getBlockPuller(): BlockPuller {
        return blockPuller
    }

    fun getBlockPusher(): BlockPusher {
        return blockPusher
    }

    private fun sendIndexMessage(folderId: String) {
        sendMessage(Index.newBuilder()
                .setFolder(folderId)
                .build())
    }

    fun closeBg() {
        Thread { close() }.start()
    }

    /**
     * Receive hello message and save device name to configuration.
     */
    @Throws(IOException::class)
    private fun receiveHelloMessage() {
        val hello = HelloMessageHandler.receiveHelloMessage(inputStream!!)

        HelloMessageHandler.processHelloMessage(hello, configuration, deviceId())
    }

    private fun sendHelloMessageInBackground(): Future<*> {
        return outExecutorService.submitLogging {
            try {
                HelloMessageHandler.sendHelloMessage(configuration, outputStream!!)
            } catch (ex: IOException) {
                if (outExecutorService.isShutdown) {
                    return@submitLogging
                }
                logger.error("error writing to output stream", ex)
                closeBg()
            }
        }
    }

    private fun sendPing(): Future<*> {
        return sendMessage(Ping.newBuilder().build())
    }

    private fun markActivityOnSocket() {
        lastActive = System.currentTimeMillis()
    }

    private fun receiveMessage() = PostAuthenticationMessageHandler.receiveMessage(
            inputStream = inputStream!!,
            markActivityOnSocket = this::markActivityOnSocket
    )

    internal fun sendMessage(message: MessageLite): Future<*> {
        checkNotClosed()

        return outExecutorService.submit<Any> {
            try {
                PostAuthenticationMessageHandler.sendMessage(
                        outputStream = outputStream!!,
                        markActivityOnSocket = this::markActivityOnSocket,
                        message = message
                )
            } catch (ex: IOException) {
                if (!outExecutorService.isShutdown) {
                    logger.error("error writing to output stream", ex)
                    closeBg()
                }
                throw ex
            }

            null
        }
    }

    override fun close() {
        if (!isClosed) {
            sendMessage(Close.getDefaultInstance())
            isClosed = true
            isConnected = false
            periodicExecutorService.shutdown()
            outExecutorService.shutdown()
            inExecutorService.shutdown()
            messageProcessingService.shutdown()
            assert(onRequestMessageReceivedListeners.isEmpty())
            if (outputStream != null) {
                IOUtils.closeQuietly(outputStream)
                outputStream = null
            }
            if (inputStream != null) {
                IOUtils.closeQuietly(inputStream)
                inputStream = null
            }
            try {
              IOUtils.closeQuietly(socket)
            } catch (ex: Exception) {
              // ignore this
              // this can throw an exception if socket was not yet initialized/ set
              // as Kotlin does an check about this, the closeQuietly does not catch it
            }
            logger.info("closed connection {}", address)
            synchronized(clusterConfigWaitingLock) {
                clusterConfigWaitingLock.notifyAll()
            }
            onConnectionChangedListener(this)
            try {
                periodicExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                outExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                inExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                messageProcessingService.awaitTermination(2, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

        }
    }

    /**
     * return time elapsed since last activity on socket, inputStream millis
     *
     * @return
     */
    fun getLastActive(): Long {
        return System.currentTimeMillis() - lastActive
    }

    private fun startMessageListenerService() {
        inExecutorService.submitLogging {
            try {
                while (!Thread.interrupted()) {
                    val message = receiveMessage()
                    messageProcessingService.submitLogging {
                        logger.debug("received message type = {} {}", message.first, getIdForMessage(message.second))
                        when (message.first) {
                            BlockExchangeProtos.MessageType.INDEX -> {
                                val index = message.second as Index
                                indexHandler.handleIndexMessageReceivedEvent(index.folder, index.filesList, clusterConfigInfo!!, deviceId())
                            }
                            BlockExchangeProtos.MessageType.INDEX_UPDATE -> {
                                val update = message.second as IndexUpdate
                                indexHandler.handleIndexMessageReceivedEvent(update.folder, update.filesList, clusterConfigInfo!!, deviceId())
                            }
                            BlockExchangeProtos.MessageType.REQUEST -> {
                                onRequestMessageReceivedListeners.forEach { it(message.second as Request) }
                            }
                            BlockExchangeProtos.MessageType.RESPONSE -> {
                                responseHandler.handleResponse(message.second as Response)
                            }
                            BlockExchangeProtos.MessageType.PING -> logger.debug("ping message received")
                            BlockExchangeProtos.MessageType.CLOSE -> {
                                val close = message.second as BlockExchangeProtos.Close
                                logger.info("received close message, reason=${close.reason}")
                                closeBg()
                            }
                            BlockExchangeProtos.MessageType.CLUSTER_CONFIG -> {
                                NetworkUtils.assertProtocol(clusterConfigInfo == null, {"received cluster config message twice!"})
                                val clusterConfig = message.second as ClusterConfig

                                clusterConfigInfo = ClusterConfigHandler.handleReceivedClusterConfig(
                                        clusterConfig = clusterConfig,
                                        configuration = configuration,
                                        otherDeviceId = deviceId(),
                                        onNewFolderSharedListener = { onNewFolderSharedListener(this, it) },
                                        indexHandler = indexHandler
                                )

                                synchronized(clusterConfigWaitingLock) {
                                    clusterConfigWaitingLock.notifyAll()
                                }
                            }
                        }
                    }
                }
            } catch (ex: IOException) {
                if (inExecutorService.isShutdown) {
                    return@submitLogging
                }
                logger.error("error receiving message", ex)
                closeBg()
            }
        }
    }

    override fun toString(): String {
        return "ConnectionHandler{" + "address=" + address + ", lastActive=" + getLastActive() / 1000.0 + "secs ago}"
    }

    fun hasFolder(folder: String): Boolean {
        return clusterConfigInfo!!.getSharedFolders().contains(folder)
    }

    companion object {
        // TODO: move this somewhere else
        /**
         * get id for message bean/instance, for log tracking
         *
         * @param message
         * @return id for message bean
         */
        fun getIdForMessage(message: MessageLite): String {
            return when (message) {
                is Request -> Integer.toString(message.id)
                is Response -> Integer.toString(message.id)
                else -> Integer.toString(Math.abs(message.hashCode()))
            }
        }

        private val logger = LoggerFactory.getLogger(ConnectionHandler::class.java)

    }

    data class MessageTypeInfo(
            val protoMessageType: MessageType,
            val javaClass: Class<out MessageLite>,
            val parseFrom: (data: ByteArray) -> MessageLite
    )
}
