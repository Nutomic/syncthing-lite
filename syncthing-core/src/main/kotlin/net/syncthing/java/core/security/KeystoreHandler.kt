/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.security

import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.RelayConnection
import net.syncthing.java.core.utils.NetworkUtils
import org.apache.commons.codec.binary.Base32
import org.apache.commons.lang3.tuple.Pair
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v1CertificateBuilder
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.util.encoders.Base64
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.math.BigInteger
import java.net.InetSocketAddress
import java.net.Socket
import java.security.*
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.*
import java.util.concurrent.TimeUnit
import javax.net.ssl.*
import javax.security.auth.x500.X500Principal

class KeystoreHandler private constructor(private val keyStore: KeyStore) {

    class CryptoException internal constructor(t: Throwable) : GeneralSecurityException(t)

    private val socketFactory: SSLSocketFactory

    init {
        val sslContext = SSLContext.getInstance(TLS_VERSION)
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        keyManagerFactory.init(keyStore, KEY_PASSWORD.toCharArray())

        sslContext.init(keyManagerFactory.keyManagers, arrayOf(object : X509TrustManager {
            @Throws(CertificateException::class)
            override fun checkClientTrusted(xcs: Array<X509Certificate>, string: String) {}
            @Throws(CertificateException::class)
            override fun checkServerTrusted(xcs: Array<X509Certificate>, string: String) {}
            override fun getAcceptedIssuers() = arrayOf<X509Certificate>()
        }), null)
        socketFactory = sslContext.socketFactory
    }

    @Throws(CryptoException::class, IOException::class)
    private fun exportKeystoreToData(): ByteArray {
        val out = ByteArrayOutputStream()
        try {
            keyStore.store(out, JKS_PASSWORD.toCharArray())
        } catch (ex: NoSuchAlgorithmException) {
            throw CryptoException(ex)
        } catch (ex: CertificateException) {
            throw CryptoException(ex)
        }
        return out.toByteArray()
    }

    @Throws(CryptoException::class, IOException::class)
    private fun wrapSocket(socket: Socket, isServerSocket: Boolean, protocol: String): SSLSocket {
        try {
            logger.debug("wrapping plain socket, server mode = {}", isServerSocket)
            val sslSocket = socketFactory.createSocket(socket, null, socket.port, true) as SSLSocket
            if (isServerSocket) {
                sslSocket.useClientMode = false
            }
            return sslSocket
        } catch (e: KeyManagementException) {
            throw CryptoException(e)
        } catch (e: NoSuchAlgorithmException) {
            throw CryptoException(e)
        } catch (e: KeyStoreException) {
            throw CryptoException(e)
        } catch (e: UnrecoverableKeyException) {
            throw CryptoException(e)
        }

    }

    @Throws(CryptoException::class, IOException::class)
    fun createSocket(relaySocketAddress: InetSocketAddress, protocol: String): SSLSocket {
        try {
            val socket = socketFactory.createSocket() as SSLSocket
            socket.connect(relaySocketAddress, SOCKET_TIMEOUT)
            return socket
        } catch (e: KeyManagementException) {
            throw CryptoException(e)
        } catch (e: NoSuchAlgorithmException) {
            throw CryptoException(e)
        } catch (e: KeyStoreException) {
            throw CryptoException(e)
        } catch (e: UnrecoverableKeyException) {
            throw CryptoException(e)
        }
    }

    @Throws(CryptoException::class, IOException::class)
    fun wrapSocket(relayConnection: RelayConnection, protocol: String): SSLSocket {
        return wrapSocket(relayConnection.getSocket(), relayConnection.isServerSocket(), protocol)
    }

    class Loader {

        private val logger = LoggerFactory.getLogger(javaClass)

        private fun getKeystoreAlgorithm(keystoreAlgorithm: String?): String {
            return keystoreAlgorithm?.let { algo ->
                if (!algo.isBlank()) algo else null
            } ?: {
                val defaultAlgo = KeyStore.getDefaultType()!!
                logger.debug("keystore algo set to {}", defaultAlgo)
                defaultAlgo
            }()
        }

        @Throws(CryptoException::class, IOException::class)
        fun generateKeystore(): Triple<DeviceId, ByteArray, String> {
            val keystoreAlgorithm = getKeystoreAlgorithm(null)
            val keystore = generateKeystore(keystoreAlgorithm)
            val keystoreHandler = KeystoreHandler(keystore.left)
            val keystoreData = keystoreHandler.exportKeystoreToData()
            val hash = MessageDigest.getInstance("SHA-256").digest(keystoreData)
            keystoreHandlersCacheByHash[Base32().encodeAsString(hash)] = keystoreHandler
            logger.info("keystore ready, device id = {}", keystore.right)
            return Triple(keystore.right, keystoreData, keystoreAlgorithm)
        }

        fun loadKeystore(configuration: Configuration): KeystoreHandler {
            val hash = MessageDigest.getInstance("SHA-256").digest(configuration.keystoreData)
            val keystoreHandlerFromCache = keystoreHandlersCacheByHash[Base32().encodeAsString(hash)]
            if (keystoreHandlerFromCache != null) {
                return keystoreHandlerFromCache
            }
            val keystoreAlgo = getKeystoreAlgorithm(configuration.keystoreAlgorithm)
            val keystore = importKeystore(configuration.keystoreData, keystoreAlgo)
            val keystoreHandler = KeystoreHandler(keystore.left)
            keystoreHandlersCacheByHash[Base32().encodeAsString(hash)] = keystoreHandler
            logger.info("keystore ready, device id = {}", keystore.right)
            return keystoreHandler
        }

        @Throws(CryptoException::class, IOException::class)
        private fun generateKeystore(keystoreAlgorithm: String): Pair<KeyStore, DeviceId> {
            try {
                logger.debug("generating key")
                val keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGO)
                keyPairGenerator.initialize(KEY_SIZE)
                val keyPair = keyPairGenerator.genKeyPair()

                val contentSigner = JcaContentSignerBuilder(SIGNATURE_ALGO).build(keyPair.private)

                val startDate = Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                val endDate = Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(10 * 365))

                val certificateBuilder = JcaX509v1CertificateBuilder(X500Principal(CERTIFICATE_CN), BigInteger.ZERO,
                        startDate, endDate, X500Principal(CERTIFICATE_CN), keyPair.public)

                val certificateHolder = certificateBuilder.build(contentSigner)

                val certificateDerData = certificateHolder.encoded
                logger.info("generated cert =\n{}", derToPem(certificateDerData))
                val deviceId = derDataToDeviceId(certificateDerData)
                logger.info("device id from cert = {}", deviceId)

                val keyStore = KeyStore.getInstance(keystoreAlgorithm)
                keyStore.load(null, null)
                val certChain = arrayOfNulls<Certificate>(1)
                certChain[0] = JcaX509CertificateConverter().getCertificate(certificateHolder)
                keyStore.setKeyEntry("key", keyPair.private, KEY_PASSWORD.toCharArray(), certChain)
                return Pair.of(keyStore, deviceId)
            } catch (e: OperatorCreationException) {
                throw CryptoException(e)
            } catch (e: CertificateException) {
                throw CryptoException(e)
            } catch (e: NoSuchAlgorithmException) {
                throw CryptoException(e)
            } catch (e: KeyStoreException) {
                throw CryptoException(e)
            }

        }

        @Throws(CryptoException::class, IOException::class)
        private fun importKeystore(keystoreData: ByteArray, keystoreAlgorithm: String): Pair<KeyStore, DeviceId> {
            try {
                val keyStore = KeyStore.getInstance(keystoreAlgorithm)
                keyStore.load(ByteArrayInputStream(keystoreData), JKS_PASSWORD.toCharArray())
                val alias = keyStore.aliases().nextElement()
                val certificate = keyStore.getCertificate(alias)
                NetworkUtils.assertProtocol(certificate is X509Certificate)
                val derData = certificate.encoded
                val deviceId = derDataToDeviceId(derData)
                logger.debug("loaded device id from cert = {}", deviceId)
                return Pair.of(keyStore, deviceId)
            } catch (e: NoSuchAlgorithmException) {
                throw CryptoException(e)
            } catch (e: KeyStoreException) {
                throw CryptoException(e)
            } catch (e: CertificateException) {
                throw CryptoException(e)
            }

        }

        companion object {
            private val keystoreHandlersCacheByHash = mutableMapOf<String, KeystoreHandler>()
        }
    }

    companion object {

        private const val JKS_PASSWORD = "password"
        private const val KEY_PASSWORD = "password"
        private const val KEY_ALGO = "RSA"
        private const val SIGNATURE_ALGO = "SHA1withRSA"
        private const val CERTIFICATE_CN = "CN=syncthing"
        private const val KEY_SIZE = 3072
        private const val SOCKET_TIMEOUT = 2000
        private const val TLS_VERSION = "TLSv1.2"

        init {
            Security.addProvider(BouncyCastleProvider())
        }

        private fun derToPem(der: ByteArray): String {
            return "-----BEGIN CERTIFICATE-----\n" + Base64.toBase64String(der).chunked(76).joinToString("\n") + "\n-----END CERTIFICATE-----"
        }

        fun derDataToDeviceId(certificateDerData: ByteArray): DeviceId {
            return DeviceId.fromHashData(MessageDigest.getInstance("SHA-256").digest(certificateDerData))
        }

        const val BEP = "bep/1.0"
        const val RELAY = "bep-relay"

        private val logger = LoggerFactory.getLogger(KeystoreHandler::class.java)

        @Throws(SSLPeerUnverifiedException::class, CertificateException::class)
        fun assertSocketCertificateValid(socket: SSLSocket, deviceId: DeviceId) {
            val session = socket.session
            val certs = session.peerCertificates.toList()
            val certificateFactory = CertificateFactory.getInstance("X.509")
            val certPath = certificateFactory.generateCertPath(certs)
            val certificate = certPath.certificates[0]

            assertSocketCertificateValid(certificate, deviceId)
        }

        @Throws(SSLPeerUnverifiedException::class, CertificateException::class)
        fun assertSocketCertificateValid(certificate: Certificate, deviceId: DeviceId) {
            NetworkUtils.assertProtocol(certificate is X509Certificate)
            val derData = certificate.encoded
            val deviceIdFromCertificate = derDataToDeviceId(derData)
            logger.trace("remote pem certificate =\n{}", derToPem(derData))
            NetworkUtils.assertProtocol(deviceIdFromCertificate == deviceId, {"device id mismatch! expected = $deviceId, got = $deviceIdFromCertificate"})
            logger.debug("remote ssl certificate match deviceId = {}", deviceId)
        }
    }

}
