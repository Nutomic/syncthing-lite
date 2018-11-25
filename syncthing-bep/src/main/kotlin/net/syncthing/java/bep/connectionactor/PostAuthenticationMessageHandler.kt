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
import net.jpountz.lz4.LZ4Factory
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.utils.NetworkUtils
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer

object PostAuthenticationMessageHandler {
    private val logger = LoggerFactory.getLogger(PostAuthenticationMessageHandler::class.java)

    fun sendMessage(
            outputStream: DataOutputStream,
            message: MessageLite,
            markActivityOnSocket: () -> Unit
    ) {
        val messageTypeInfo = MessageTypes.messageTypesByJavaClass[message.javaClass]!!
        val header = BlockExchangeProtos.Header.newBuilder()
                .setCompression(BlockExchangeProtos.MessageCompression.NONE)
                .setType(messageTypeInfo.protoMessageType)
                .build()
        val headerData = header.toByteArray()
        val messageData = message.toByteArray() //TODO support compression

        logger.debug("sending message type = {} {}", header.type, MessageTypes.getIdForMessage(message))
        markActivityOnSocket()

        outputStream.apply {
            writeShort(headerData.size)
            write(headerData)
            writeInt(messageData.size)
            write(messageData)
            flush()
        }

        markActivityOnSocket()
    }

    fun receiveMessage(
            inputStream: DataInputStream,
            markActivityOnSocket: () -> Unit
    ): Pair<BlockExchangeProtos.MessageType, MessageLite> {
        val header = BlockExchangeProtos.Header.parseFrom(readHeader(
                inputStream = inputStream,
                retryReadingLength = true,
                markActivityOnSocket = markActivityOnSocket
        ))

        var messageBuffer = readMessage(
                inputStream = inputStream,
                retryReadingLength = true,
                markActivityOnSocket = markActivityOnSocket
        )

        if (header.compression == BlockExchangeProtos.MessageCompression.LZ4) {
            val uncompressedLength = ByteBuffer.wrap(messageBuffer).int
            messageBuffer = LZ4Factory.fastestInstance().fastDecompressor().decompress(messageBuffer, 4, uncompressedLength)
        }

        val messageTypeInfo = MessageTypes.messageTypesByProtoMessageType[header.type]
        NetworkUtils.assertProtocol(messageTypeInfo != null) {"unsupported message type = ${header.type}"}

        try {
            return header.type to messageTypeInfo!!.parseFrom(messageBuffer)
        } catch (e: Exception) {
            when (e) {
                is IllegalAccessException, is IllegalArgumentException, is InvocationTargetException, is NoSuchMethodException, is SecurityException ->
                    throw IOException(e)
                else -> throw e
            }
        }
    }

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

        NetworkUtils.assertProtocol(messageLength >= 0) {"invalid length, must be >= 0, got $messageLength"}

        val messageBuffer = ByteArray(messageLength)
        inputStream.readFully(messageBuffer)
        markActivityOnSocket()

        return messageBuffer
    }
}
