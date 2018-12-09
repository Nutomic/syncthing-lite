package net.syncthing.java.core.configuration

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.security.KeystoreHandler
import org.bouncycastle.util.encoders.Base64
import org.slf4j.LoggerFactory
import java.io.File
import java.io.StringReader
import java.io.StringWriter
import java.net.InetAddress
import java.util.*

class Configuration(configFolder: File = DefaultConfigFolder) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val modifyLock = Mutex()
    private val saveLock = Mutex()
    private val configChannel = ConflatedBroadcastChannel<Config>()

    private val configFile = File(configFolder, ConfigFileName)
    val databaseFolder = File(configFolder, DatabaseFolderName)

    private var isSaved = true

    init {
        configFolder.mkdirs()
        databaseFolder.mkdirs()
        assert(configFolder.isDirectory && configFile.canWrite(), { "Invalid config folder $configFolder" })

        if (!configFile.exists()) {
            var localDeviceName = InetAddress.getLocalHost().hostName
            if (localDeviceName.isEmpty() || localDeviceName == "localhost") {
                localDeviceName = "syncthing-lite"
            }
            val keystoreData = KeystoreHandler.Loader().generateKeystore()
            isSaved = false
            configChannel.sendBlocking(
                    Config(peers = setOf(), folders = setOf(),
                            localDeviceName = localDeviceName,
                            localDeviceId = keystoreData.first.deviceId,
                            keystoreData = Base64.toBase64String(keystoreData.second),
                            keystoreAlgorithm = keystoreData.third,
                            customDiscoveryServers = emptySet(),
                            useDefaultDiscoveryServers = true
                    )
            )
            runBlocking { persistNow() }
        } else {
            configChannel.sendBlocking(
                    Config.parse(JsonReader(StringReader(configFile.readText())))
            )
        }
        logger.debug("Loaded config = ${configChannel.value}")
    }

    companion object {
        private val DefaultConfigFolder = File(System.getProperty("user.home"), ".config/syncthing-java/")
        private const val ConfigFileName = "config.json"
        private const val DatabaseFolderName = "database"
    }

    val instanceId = Math.abs(Random().nextLong())

    val localDeviceId: DeviceId
        get() = DeviceId(configChannel.value.localDeviceId)

    val discoveryServers: Set<DiscoveryServer>
        get() = configChannel.value.let { config ->
            config.customDiscoveryServers + (if (config.useDefaultDiscoveryServers) DiscoveryServer.defaultDiscoveryServers else emptySet())
        }

    val keystoreData: ByteArray
        get() = Base64.decode(configChannel.value.keystoreData)

    val keystoreAlgorithm: String
        get() = configChannel.value.keystoreAlgorithm

    val clientName = "syncthing-java"

    val clientVersion = javaClass.`package`.implementationVersion ?: "0.0.0"

    val peerIds: Set<DeviceId>
        get() = configChannel.value.peers.map { it.deviceId }.toSet()

    val localDeviceName: String
        get() = configChannel.value.localDeviceName

    val folders: Set<FolderInfo>
        get() = configChannel.value.folders

    val peers: Set<DeviceInfo>
        get() = configChannel.value.peers

    suspend fun update(operation: suspend (Config) -> Config): Boolean {
        modifyLock.withLock {
            val oldConfig = configChannel.value
            val newConfig = operation(oldConfig)

            if (oldConfig != newConfig) {
                configChannel.send(newConfig)
                isSaved = false

                return true
            } else {
                return false
            }
        }
    }

    suspend fun persistNow() {
        persist()
    }

    fun persistLater() {
        GlobalScope.launch (Dispatchers.IO) { persist() }
    }

    private suspend fun persist() {
        saveLock.withLock {
            val (config1, isConfig1Saved) = modifyLock.withLock { configChannel.value to isSaved }

            if (isConfig1Saved) {
                return
            }

            System.out.println("writing config to $configFile")

            configFile.writeText(
                    StringWriter().apply {
                        JsonWriter(this).apply {
                            setIndent("  ")

                            config1.serialize(this)
                        }
                    }.toString()
            )

            modifyLock.withLock {
                if (config1 === configChannel.value) {
                    isSaved = true
                }
            }
        }
    }

    fun subscribe() = configChannel.openSubscription()

    override fun toString() = "Configuration(peers=$peers, folders=$folders, localDeviceName=$localDeviceName, " +
            "localDeviceId=${localDeviceId.deviceId}, discoveryServers=$discoveryServers, instanceId=$instanceId, " +
            "configFile=$configFile, databaseFolder=$databaseFolder)"
}
