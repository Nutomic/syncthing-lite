package net.syncthing.java.bep.connectionactor

import net.syncthing.java.core.beans.DeviceAddress

data class ConnectionInfo (
        val addresses: List<DeviceAddress>,
        val currentAddress: DeviceAddress?,
        val status: ConnectionStatus
) {
    companion object {
        val empty = ConnectionInfo(
                addresses = emptyList(),
                currentAddress = null,
                status = ConnectionStatus.Disconnected
        )
    }
}

enum class ConnectionStatus {
    Disconnected, Connecting, Connected
}
