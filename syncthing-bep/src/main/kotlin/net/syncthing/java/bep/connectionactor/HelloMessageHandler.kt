package net.syncthing.java.bep.connectionactor

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.ConnectionHandler
import net.syncthing.java.core.configuration.Configuration
import org.slf4j.LoggerFactory
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
                        putInt(ConnectionHandler.MAGIC)
                        putShort(payload.size.toShort())
                    }.array()
            )
            write(payload)
            flush()
        }
    }
}
