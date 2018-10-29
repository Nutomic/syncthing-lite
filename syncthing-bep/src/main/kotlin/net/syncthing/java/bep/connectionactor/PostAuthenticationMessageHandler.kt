package net.syncthing.java.bep.connectionactor

import com.google.protobuf.MessageLite
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.ConnectionHandler
import org.slf4j.LoggerFactory
import java.io.DataOutputStream

object PostAuthenticationMessageHandler {
    private val logger = LoggerFactory.getLogger(PostAuthenticationMessageHandler::class.java)

    fun sendMessage(
            outputStream: DataOutputStream,
            message: MessageLite,
            markActivityOnSocket: () -> Unit
    ) {
        val messageTypeInfo = ConnectionHandler.messageTypesByJavaClass[message.javaClass]!!
        val header = BlockExchangeProtos.Header.newBuilder()
                .setCompression(BlockExchangeProtos.MessageCompression.NONE)
                .setType(messageTypeInfo.protoMessageType)
                .build()
        val headerData = header.toByteArray()
        val messageData = message.toByteArray() //TODO support compression

        logger.debug("sending message type = {} {}", header.type, ConnectionHandler.getIdForMessage(message))
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
}
