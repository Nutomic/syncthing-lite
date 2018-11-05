package net.syncthing.java.bep

import net.syncthing.java.core.beans.DeviceId

class NewBlockPusher {
    suspend fun handleRequest(source: DeviceId, request: BlockExchangeProtos.Request): BlockExchangeProtos.Response {
        // TODO: add possibility to add listeners

        return BlockExchangeProtos.Response.newBuilder()
                .setId(request.id)
                .setCode(BlockExchangeProtos.ErrorCode.GENERIC)
                .build()
    }
}
