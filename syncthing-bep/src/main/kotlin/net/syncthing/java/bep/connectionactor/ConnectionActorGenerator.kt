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
            deviceAddressSource: ReceiveChannel<List<DeviceAddress>>,
            configuration: Configuration,
            indexHandler: IndexHandler,
            requestHandler: (BlockExchangeProtos.Request) -> Deferred<BlockExchangeProtos.Response>
    ) = GlobalScope.produce {
        var currentActor: SendChannel<ConnectionAction> = closed
        var currentDeviceAddress: DeviceAddress? = null

        suspend fun closeCurrent() {
            if (currentActor != closed) {
                currentActor.close()
                currentActor = closed
                send(currentActor)
            }
        }

        invokeOnClose { currentActor.close() }

        deviceAddressSource.consumeEach { deviceAddresses ->
            if (deviceAddresses.isEmpty()) {
                closeCurrent()
            } else {
                if (currentDeviceAddress == deviceAddresses.first() && (!currentActor.isClosedForSend)) {
                    // don't reconnect
                    return@consumeEach
                }

                for (deviceAddress in deviceAddresses) {
                    closeCurrent()

                    if (!deviceAddressSource.isEmpty) {
                        // time to consume the next value
                        break
                    }

                    val newActor = ConnectionActor.createInstance(deviceAddress, configuration, indexHandler, requestHandler)

                    try {
                        ConnectionActorUtil.waitUntilConnected(newActor)
                    } catch (ex: Exception) {
                        // TODO: catch more specific
                        // TODO: log exception?

                        continue
                    }

                    currentActor = newActor
                    currentDeviceAddress = deviceAddress

                    send(newActor)

                    break   // don't try the other addresses
                }
            }
        }
    }
}
