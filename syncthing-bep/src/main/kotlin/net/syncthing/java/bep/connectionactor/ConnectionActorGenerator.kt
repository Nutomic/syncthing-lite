package net.syncthing.java.bep.connectionactor

import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.channels.*
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration

object ConnectionActorGenerator {
    private val closed = Channel<ConnectionAction>().apply { cancel() }

    fun generateConnectionActors(
            deviceAddressSource: ReceiveChannel<DeviceAddress?>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = GlobalScope.produce {
        var currentActor: SendChannel<ConnectionAction> = closed

        invokeOnClose { currentActor.close() }

        deviceAddressSource.consumeEach { deviceAddress ->
            if (currentActor != closed) {
                currentActor = closed
                send(currentActor)
            }

            if (deviceAddress != null) {
                val newActor = ConnectionActor.createInstance(deviceAddress, configuration, indexHandler, requestHandler)

                try {
                    ConnectionActorUtil.waitUntilConnected(newActor)

                    currentActor = newActor
                    send(newActor)
                } catch (ex: Exception) {
                    // TODO: log exception
                    // TODO: try other device address?
                }
            }
        }
    }
}
