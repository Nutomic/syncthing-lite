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

import com.google.protobuf.ByteString
import net.syncthing.java.core.interfaces.RelayConnection
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.core.utils.submitLogging
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import java.io.*
import java.net.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class HttpRelayConnection internal constructor(private val httpRelayServerUrl: String, deviceId: String) : RelayConnection, Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val outgoingExecutorService = Executors.newSingleThreadExecutor()
    private val incomingExecutorService = Executors.newSingleThreadExecutor()
    private val flusherStreamService = Executors.newSingleThreadScheduledExecutor()
    private var peerToRelaySequence: Long = 0
    private var relayToPeerSequence: Long = 0
    private val sessionId: String
    private val incomingDataQueue = LinkedBlockingQueue<ByteArray>()
    private val socket: Socket
    private val isServerSocket: Boolean
    private val inputStream: InputStream
    private val outputStream: OutputStream

    var isClosed = false
        private set

    override fun getSocket() = socket

    override fun isServerSocket() = isServerSocket

    init {
        val serverMessage = sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
                .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.CONNECT)
                .setDeviceId(deviceId))
        assert(serverMessage.messageType == HttpRelayProtos.HttpRelayServerMessageType.PEER_CONNECTED)
        assert(!serverMessage.sessionId.isNullOrEmpty())
        sessionId = serverMessage.sessionId
        isServerSocket = serverMessage.isServerSocket
        outputStream = object : OutputStream() {

            private var buffer = ByteArrayOutputStream()
            private var lastFlush = System.currentTimeMillis()

            init {
                flusherStreamService.scheduleWithFixedDelay({
                    if (System.currentTimeMillis() - lastFlush > 1000) {
                        try {
                            flush()
                        } catch (ex: IOException) {
                            logger.warn("", ex)
                        }

                    }
                }, 1, 1, TimeUnit.SECONDS)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(i: Int) {
                NetworkUtils.assertProtocol(!this@HttpRelayConnection.isClosed)
                buffer.write(i)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(bytes: ByteArray, offset: Int, size: Int) {
                NetworkUtils.assertProtocol(!this@HttpRelayConnection.isClosed)
                buffer.write(bytes, offset, size)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun flush() {
                val data = buffer.toByteArray().copyOf().toList()
                buffer = ByteArrayOutputStream()
                try {
                    if (!data.isEmpty()) {
                        outgoingExecutorService.submit {
                            sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
                                    .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_TO_RELAY)
                                    .setSequence(++peerToRelaySequence)
                                    .setData(data as ByteString))
                        }.get()
                    }
                    lastFlush = System.currentTimeMillis()
                } catch (ex: InterruptedException) {
                    logger.error("error", ex)
                    closeBg()
                    throw IOException(ex)
                } catch (ex: ExecutionException) {
                    logger.error("error", ex)
                    closeBg()
                    throw IOException(ex)
                }

            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(bytes: ByteArray) {
                NetworkUtils.assertProtocol(!this@HttpRelayConnection.isClosed)
                buffer.write(bytes)
            }

        }
        incomingExecutorService.submitLogging {
            while (!isClosed) {
                val serverMessage1 =
                    try {
                        sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.WAIT_FOR_DATA))
                    } catch (e: IOException) {
                        logger.warn("Failed to send relay message", e)
                        return@submitLogging
                    }
                if (isClosed) {
                    return@submitLogging
                }
                NetworkUtils.assertProtocol(serverMessage1.messageType == HttpRelayProtos.HttpRelayServerMessageType.RELAY_TO_PEER)
                NetworkUtils.assertProtocol(serverMessage1.sequence == relayToPeerSequence + 1)
                if (!serverMessage1.data.isEmpty) {
                    incomingDataQueue.add(serverMessage1.data.toByteArray())
                }
                relayToPeerSequence = serverMessage1.sequence
            }
        }
        inputStream = object : InputStream() {

            private var noMoreData = false
            private var byteArrayInputStream = ByteArrayInputStream(ByteArray(0))

            @Throws(IOException::class)
            override fun read(): Int {
                NetworkUtils.assertProtocol(!this@HttpRelayConnection.isClosed)
                if (noMoreData) {
                    return -1
                }
                var bite = -1
                while (bite == -1) {
                    bite = byteArrayInputStream.read()
                    try {
                        val data = incomingDataQueue.poll(1, TimeUnit.SECONDS)
                        if (data == null) {
                            //continue
                        } else if (data.contentEquals(STREAM_CLOSED)) {
                            noMoreData = true
                            return -1
                        } else {
                            byteArrayInputStream = ByteArrayInputStream(data)
                        }
                    } catch (ex: InterruptedException) {
                        logger.warn("", ex)
                    }

                }
                return bite
            }

        }
        socket = object : Socket() {
            override fun isClosed(): Boolean {
                return this@HttpRelayConnection.isClosed
            }

            override fun isConnected(): Boolean {
                return !isClosed
            }

            @Throws(IOException::class)
            override fun shutdownOutput() {
                logger.debug("shutdownOutput")
                outputStream.flush()
            }

            @Throws(IOException::class)
            override fun shutdownInput() {
                logger.debug("shutdownInput")
                //do nothing
            }

            @Synchronized
            @Throws(IOException::class)
            override fun close() {
                logger.debug("received close on socket adapter")
                this@HttpRelayConnection.close()
            }

            @Throws(IOException::class)
            override fun getOutputStream(): OutputStream {
                return this@HttpRelayConnection.outputStream
            }

            @Throws(IOException::class)
            override fun getInputStream(): InputStream {
                return this@HttpRelayConnection.inputStream
            }

            @Throws(UnknownHostException::class)
            override fun getRemoteSocketAddress(): SocketAddress {
                return InetSocketAddress(inetAddress, port)
            }

            override fun getPort(): Int {
                return 22067
            }

            @Throws(UnknownHostException::class)
            override fun getInetAddress(): InetAddress {
                return InetAddress.getByName(URI.create(this@HttpRelayConnection.httpRelayServerUrl).host)
            }

        }
    }

    private fun closeBg() {

        Thread { close() }.start()
    }

    @Throws(IOException::class)
    private fun sendMessage(peerMessageBuilder: HttpRelayProtos.HttpRelayPeerMessage.Builder): HttpRelayProtos.HttpRelayServerMessage {
        if (!sessionId.isEmpty()) {
            peerMessageBuilder.sessionId = sessionId
        }
        logger.debug("send http relay peer message = {} session id = {} sequence = {}", peerMessageBuilder.messageType, peerMessageBuilder.sessionId, peerMessageBuilder.sequence)
        val httpClient = HttpClients.custom()
                //                .setSSLSocketFactory(new SSLConnectionSocketFactory(new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER))
                .build()
        val httpPost = HttpPost(httpRelayServerUrl)
        httpPost.entity = ByteArrayEntity(peerMessageBuilder.build().toByteArray())
        val serverMessage = httpClient.execute(httpPost) { response ->
            NetworkUtils.assertProtocol(response.statusLine.statusCode == HttpStatus.SC_OK, {"http error ${response.statusLine}"})
            HttpRelayProtos.HttpRelayServerMessage.parseFrom(EntityUtils.toByteArray(response.entity))
        }
        logger.debug("received http relay server message = {}", serverMessage.messageType)
        NetworkUtils.assertProtocol(serverMessage.messageType != HttpRelayProtos.HttpRelayServerMessageType.ERROR, {"server error : ${serverMessage.data.toStringUtf8()}"})
        return serverMessage
    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            logger.info("closing http relay connection {} : {}", httpRelayServerUrl, sessionId)
            flusherStreamService.shutdown()
            if (!sessionId.isEmpty()) {
                try {
                    outputStream.flush()
                    sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_CLOSING))
                } catch (ex: IOException) {
                    logger.warn("error closing http relay connection", ex)
                }

            }
            incomingExecutorService.shutdown()
            outgoingExecutorService.shutdown()
            try {
                incomingExecutorService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            try {
                outgoingExecutorService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            try {
                flusherStreamService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            incomingDataQueue.add(STREAM_CLOSED)
        }
    }

    companion object {
        private val STREAM_CLOSED = "STREAM_CLOSED".toByteArray()
    }
}
