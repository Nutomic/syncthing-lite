package net.syncthing.java.bep

import kotlinx.coroutines.Deferred
import net.syncthing.java.core.beans.DeviceId
import java.io.IOException

class RequestHandlerRegistry {
    private val listeners = mutableMapOf<RequestHandlerFilter, (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>>()

    suspend fun handleRequest(source: DeviceId, request: BlockExchangeProtos.Request): BlockExchangeProtos.Response {
        val rule = RequestHandlerFilter(
                deviceId = source,
                folderId = request.folder,
                path = request.name
        )

        val matchingListener = synchronized(listeners) {
            listeners[rule]
        }

        if (matchingListener != null) {
            return matchingListener(request).await()
        } else {
            return BlockExchangeProtos.Response.newBuilder()
                    .setId(request.id)
                    .setCode(BlockExchangeProtos.ErrorCode.GENERIC)
                    .build()
        }
    }

    fun registerListener(filter: RequestHandlerFilter, listener: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>) {
        synchronized(listeners) {
            val oldListener = listeners[filter]

            if (oldListener != null) {
                throw IOException("there is already an listener for this filter")
            }

            listeners[filter] = listener
        }
    }

    fun unregisterListener(filter: RequestHandlerFilter) {
        synchronized(listeners) {
            listeners.remove(filter)
        }
    }
}

data class RequestHandlerFilter(
        val deviceId: DeviceId,
        val folderId: String,
        val path: String
)
