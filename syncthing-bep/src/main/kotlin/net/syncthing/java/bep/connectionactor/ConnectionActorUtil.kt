package net.syncthing.java.bep.connectionactor

import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.channels.SendChannel
import net.syncthing.java.bep.BlockExchangeProtos

object ConnectionActorUtil {
    suspend fun waitUntilConnected(actor: SendChannel<ConnectionAction>) {
        val deferred = CompletableDeferred<Unit?>()

        actor.send(ConfirmIsConnectedAction(deferred))
        actor.invokeOnClose { deferred.cancel() }

        deferred.await()
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request, actor: SendChannel<ConnectionAction>): BlockExchangeProtos.Response {
        val deferred = CompletableDeferred<BlockExchangeProtos.Response>()

        actor.send(SendRequestConnectionAction(request, deferred))
        actor.invokeOnClose { deferred.cancel() }

        return deferred.await()
    }
}
