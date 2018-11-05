package net.syncthing.java.client

import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.core.beans.DeviceId

class Connections (val generate: (DeviceId) -> ConnectionActorWrapper) {
    private val map = mutableMapOf<DeviceId, ConnectionActorWrapper>()

    fun getByDeviceId(deviceId: DeviceId): ConnectionActorWrapper {
        return synchronized(map) {
            val oldEntry = map[deviceId]

            if (oldEntry != null) {
                return oldEntry
            } else {
                val newEntry = generate(deviceId)

                map[deviceId] = newEntry

                return newEntry
            }
        }
    }

    fun shutdown() {
        synchronized(map) {
            map.values.forEach { it.shutdown() }
        }
    }
}
