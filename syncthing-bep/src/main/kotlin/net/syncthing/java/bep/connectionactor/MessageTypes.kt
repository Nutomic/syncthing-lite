package net.syncthing.java.bep.connectionactor

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.ConnectionHandler

object MessageTypes {
    val messageTypes = listOf(
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.CLOSE, BlockExchangeProtos.Close::class.java) { BlockExchangeProtos.Close.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.CLUSTER_CONFIG, BlockExchangeProtos.ClusterConfig::class.java) { BlockExchangeProtos.ClusterConfig.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.DOWNLOAD_PROGRESS, BlockExchangeProtos.DownloadProgress::class.java) { BlockExchangeProtos.DownloadProgress.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX, BlockExchangeProtos.Index::class.java) { BlockExchangeProtos.Index.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX_UPDATE, BlockExchangeProtos.IndexUpdate::class.java) { BlockExchangeProtos.IndexUpdate.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.PING, BlockExchangeProtos.Ping::class.java) { BlockExchangeProtos.Ping.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.REQUEST, BlockExchangeProtos.Request::class.java) { BlockExchangeProtos.Request.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.RESPONSE, BlockExchangeProtos.Response::class.java) { BlockExchangeProtos.Response.parseFrom(it) }
    )

    val messageTypesByProtoMessageType = messageTypes.map { it.protoMessageType to it }.toMap()
    val messageTypesByJavaClass = messageTypes.map { it.javaClass to it }.toMap()
}
