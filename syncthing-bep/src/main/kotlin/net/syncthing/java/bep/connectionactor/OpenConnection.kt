package net.syncthing.java.bep.connectionactor

import net.syncthing.java.bep.ConnectionHandler
import net.syncthing.java.client.protocol.rp.RelayClient
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.httprelay.HttpRelayClient
import org.slf4j.LoggerFactory
import javax.net.ssl.SSLSocket

object OpenConnection {
    private val logger = LoggerFactory.getLogger(ConnectionHandler::class.java)

    fun openSocketConnection(
            address: DeviceAddress,
            configuration: Configuration
    ): SSLSocket {
        val keystoreHandler = KeystoreHandler.Loader().loadKeystore(configuration)

        return when (address.getType()) {
            DeviceAddress.AddressType.TCP -> {
                logger.debug("opening tcp ssl connection")
                keystoreHandler.createSocket(address.getSocketAddress(), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.RELAY -> {
                logger.debug("opening relay connection")
                keystoreHandler.wrapSocket(RelayClient(configuration).openRelayConnection(address), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.HTTP_RELAY, DeviceAddress.AddressType.HTTPS_RELAY -> {
                logger.debug("opening http relay connection")
                keystoreHandler.wrapSocket(HttpRelayClient().openRelayConnection(address), KeystoreHandler.BEP)
            }
            else -> throw UnsupportedOperationException("unsupported address type ${address.getType()}")
        }
    }
}
