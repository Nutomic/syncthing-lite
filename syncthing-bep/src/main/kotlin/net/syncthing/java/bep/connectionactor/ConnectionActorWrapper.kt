package net.syncthing.java.bep.connectionactor

import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch

class ConnectionActorWrapper (private val source: ReceiveChannel<SendChannel<ConnectionAction>>) {
    private val job = Job()

    private var currentConnectionActor: SendChannel<ConnectionAction>? = null

    var isConnected = false
        get() = currentConnectionActor?.isClosedForSend == false

    init {
        GlobalScope.launch (job) {
            source.consumeEach { connectionActor ->
                currentConnectionActor = connectionActor
            }
        }
    }

    fun isConnected() {
        currentConnectionActor != null
    }

    fun shutdown() {
        job.cancel()
    }
}
