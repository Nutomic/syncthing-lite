package net.syncthing.java.bep.connectionactor

import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock

class ConnectionActorWrapper (private val source: ReceiveChannel<SendChannel<ConnectionAction>>) {
    private val job = Job()

    private val statusLock = Mutex()

    private var currentConnectionActor: SendChannel<ConnectionAction>? = null

    var isConnected = false
        private set

    init {
        GlobalScope.launch (job) {
            source.consumeEach { connectionActor ->
                statusLock.withLock {
                    isConnected = false
                    currentConnectionActor = connectionActor
                }

                async {
                    try {
                        ConnectionActorUtil.waitUntilConnected(connectionActor)

                        statusLock.withLock {
                            if (currentConnectionActor == connectionActor) {
                                isConnected = true
                            }
                        }
                    } catch (ex: Exception) {
                        // TODO log?
                        // TODO this catches eventual too broad
                    }
                }
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
