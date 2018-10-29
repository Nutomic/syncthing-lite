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
import net.jpountz.lz4.LZ4Factory
import net.syncthing.java.bep.BlockExchangeProtos.*
import net.syncthing.java.bep.connectionactor.ClusterConfigHandler
import net.syncthing.java.bep.connectionactor.HelloMessageHandler
import net.syncthing.java.bep.connectionactor.OpenConnection
import net.syncthing.java.bep.connectionactor.PostAuthenticationMessageHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.submitLogging
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import java.security.cert.CertificateException
import java.util.concurrent.ConcurrentHashMap
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

    @Throws(IOException::class)
    private fun receiveMessage(): Pair<BlockExchangeProtos.MessageType, MessageLite> {
        val header = BlockExchangeProtos.Header.parseFrom(readHeader(
                inputStream = inputStream!!,
                retryReadingLength = true,
                markActivityOnSocket = this::markActivityOnSocket
        ))

        var messageBuffer = readMessage(
                inputStream = inputStream!!,
                retryReadingLength = true,
                markActivityOnSocket = this::markActivityOnSocket
        )

        if (header.compression == BlockExchangeProtos.MessageCompression.LZ4) {
            val uncompressedLength = ByteBuffer.wrap(messageBuffer).int
            messageBuffer = LZ4Factory.fastestInstance().fastDecompressor().decompress(messageBuffer, 4, uncompressedLength)
        }

        val messageTypeInfo = messageTypesByProtoMessageType[header.type]
        NetworkUtils.assertProtocol(messageTypeInfo != null) {"unsupported message type = ${header.type}"}

        try {
            val message = messageTypeInfo!!.parseFrom(messageBuffer)
            return Pair.of(header.type, message)
        } catch (e: Exception) {
            when (e) {
                is IllegalAccessException, is IllegalArgumentException, is InvocationTargetException, is NoSuchMethodException, is SecurityException ->
                    throw IOException(e)
                else -> throw e
            }
        }
    }

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
                        logger.debug("received message type = {} {}", message.left, getIdForMessage(message.right))
                        when (message.left) {
                            BlockExchangeProtos.MessageType.INDEX -> {
                                val index = message.value as Index
                                indexHandler.handleIndexMessageReceivedEvent(index.folder, index.filesList, this)
                            }
                            BlockExchangeProtos.MessageType.INDEX_UPDATE -> {
                                val update = message.value as IndexUpdate
                                indexHandler.handleIndexMessageReceivedEvent(update.folder, update.filesList, this)
                            }
                            BlockExchangeProtos.MessageType.REQUEST -> {
                                onRequestMessageReceivedListeners.forEach { it(message.value as Request) }
                            }
                            BlockExchangeProtos.MessageType.RESPONSE -> {
                                responseHandler.handleResponse(message.value as Response)
                            }
                            BlockExchangeProtos.MessageType.PING -> logger.debug("ping message received")
                            BlockExchangeProtos.MessageType.CLOSE -> {
                                val close = message.value as BlockExchangeProtos.Close
                                logger.info("received close message, reason=${close.reason}")
                                closeBg()
                            }
                            BlockExchangeProtos.MessageType.CLUSTER_CONFIG -> {
                                NetworkUtils.assertProtocol(clusterConfigInfo == null, {"received cluster config message twice!"})
                                clusterConfigInfo = ClusterConfigInfo()
                                val clusterConfig = message.value as ClusterConfig
                                for (folder in clusterConfig.foldersList ?: emptyList()) {
                                    val folderInfo = ClusterConfigFolderInfo(folder.id, folder.label)
                                    val devicesById = (folder.devicesList ?: emptyList())
                                            .associateBy { input ->
                                                DeviceId.fromHashData(input.id!!.toByteArray())
                                            }
                                    val otherDevice = devicesById[address.deviceId()]
                                    val ourDevice = devicesById[configuration.localDeviceId]
                                    if (otherDevice != null) {
                                        folderInfo.isAnnounced = true
                                    }
                                    if (ourDevice != null) {
                                        folderInfo.isShared = true
                                        logger.info("folder shared from device = {} folder = {}", address.deviceId, folderInfo)
                                        val folderIds = configuration.folders.map { it.folderId }
                                        if (!folderIds.contains(folderInfo.folderId)) {
                                            val fi = FolderInfo(folderInfo.folderId, folderInfo.label)
                                            configuration.folders = configuration.folders + fi
                                            onNewFolderSharedListener(this, fi)
                                            logger.info("new folder shared = {}", folderInfo)
                                        }
                                    } else {
                                        logger.info("folder not shared from device = {} folder = {}", address.deviceId, folderInfo)
                                    }
                                    clusterConfigInfo!!.putFolderInfo(folderInfo)
                                }
                                configuration.persistLater()
                                indexHandler.handleClusterConfigMessageProcessedEvent(clusterConfig)
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

    internal inner class ClusterConfigInfo {

        private val folderInfoById = ConcurrentHashMap<String, ClusterConfigFolderInfo>()

        fun getSharedFolders(): Set<String> = folderInfoById.values.filter { it.isShared }.map { it.folderId }.toSet()

        fun putFolderInfo(folderInfo: ClusterConfigFolderInfo) {
            folderInfoById[folderInfo.folderId] = folderInfo
        }

    }

    fun hasFolder(folder: String): Boolean {
        return clusterConfigInfo!!.getSharedFolders().contains(folder)
    }

    companion object {
        // TODO: move these fields somewhere else
        val messageTypes = listOf(
                MessageTypeInfo(MessageType.CLOSE, Close::class.java) { Close.parseFrom(it) },
                MessageTypeInfo(MessageType.CLUSTER_CONFIG, ClusterConfig::class.java) { ClusterConfig.parseFrom(it) },
                MessageTypeInfo(MessageType.DOWNLOAD_PROGRESS, DownloadProgress::class.java) { DownloadProgress.parseFrom(it) },
                MessageTypeInfo(MessageType.INDEX, Index::class.java) { Index.parseFrom(it) },
                MessageTypeInfo(MessageType.INDEX_UPDATE, IndexUpdate::class.java) { IndexUpdate.parseFrom(it) },
                MessageTypeInfo(MessageType.PING, Ping::class.java) { Ping.parseFrom(it) },
                MessageTypeInfo(MessageType.REQUEST, Request::class.java) { Request.parseFrom(it) },
                MessageTypeInfo(MessageType.RESPONSE, Response::class.java) { Response.parseFrom(it) }
        )

        val messageTypesByProtoMessageType = messageTypes.map { it.protoMessageType to it }.toMap()
        val messageTypesByJavaClass = messageTypes.map { it.javaClass to it }.toMap()

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

        private fun readHeader(
                inputStream: DataInputStream,
                markActivityOnSocket: () -> Unit,
                retryReadingLength: Boolean
        ): ByteArray {
            var headerLength = inputStream.readShort().toInt()

            // TODO: what is this good for?
            if (retryReadingLength) {
                while (headerLength == 0) {
                    logger.warn("got headerLength == 0, skipping short")
                    headerLength = inputStream.readShort().toInt()
                }
            }

            markActivityOnSocket()

            NetworkUtils.assertProtocol(headerLength > 0) {"invalid length, must be > 0, got $headerLength"}

            return ByteArray(headerLength).apply {
                inputStream.readFully(this)
            }
        }

        private fun readMessage(
                inputStream: DataInputStream,
                markActivityOnSocket: () -> Unit,
                retryReadingLength: Boolean
        ): ByteArray {
            var messageLength = inputStream.readInt()

            // TODO: what is this good for?
            if (retryReadingLength) {
                while (messageLength == 0) {
                    logger.warn("received readInt() == 0, expecting 'bep message header length' (int >0), ignoring (keepalive?)")
                    messageLength = inputStream.readInt()
                }
            }

            NetworkUtils.assertProtocol(messageLength >= 0, {"invalid lenght, must be >=0, got $messageLength"})

            val messageBuffer = ByteArray(messageLength)
            inputStream.readFully(messageBuffer)
            markActivityOnSocket()

            return messageBuffer
        }
    }

    data class MessageTypeInfo(
            val protoMessageType: MessageType,
            val javaClass: Class<out MessageLite>,
            val parseFrom: (data: ByteArray) -> MessageLite
    )
}
