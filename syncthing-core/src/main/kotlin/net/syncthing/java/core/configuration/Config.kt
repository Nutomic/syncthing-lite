package net.syncthing.java.core.configuration

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.FolderInfo
import java.util.*
import kotlin.collections.HashSet

data class Config(
        val peers: Set<DeviceInfo>,
        val folders: Set<FolderInfo>,
        val localDeviceName: String,
        val localDeviceId: String,
        val discoveryServers: Set<String>,
        val keystoreAlgorithm: String,
        val keystoreData: String
) {
    companion object {
        private const val PEERS = "peers"
        private const val FOLDERS = "folders"
        private const val LOCAL_DEVICE_NAME = "localDeviceName"
        private const val LOCAL_DEVICE_ID = "localDeviceId"
        private const val DISCOVERY_SERVERS = "discoveryServers"
        private const val KEYSTORE_ALGORITHM = "keystoreAlgorithm"
        private const val KEYSTORE_DATA = "keystoreData"

        fun parse(reader: JsonReader): Config {
            var peers: Set<DeviceInfo>? = null
            var folders: Set<FolderInfo>? = null
            var localDeviceName: String? = null
            var localDeviceId: String? = null
            var discoveryServers: Set<String>? = null
            var keystoreAlgorithm: String? = null
            var keystoreData: String? = null

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    PEERS -> {
                        val newPeers = HashSet<DeviceInfo>()

                        reader.beginArray()
                        while (reader.hasNext()) {
                            newPeers.add(DeviceInfo.parse(reader))
                        }
                        reader.endArray()

                        peers = Collections.unmodifiableSet(newPeers)
                    }
                    FOLDERS -> {
                        val newFolders = HashSet<FolderInfo>()

                        reader.beginArray()
                        while (reader.hasNext()) {
                            newFolders.add(FolderInfo.parse(reader))
                        }
                        reader.endArray()

                        folders = Collections.unmodifiableSet(newFolders)
                    }
                    LOCAL_DEVICE_NAME -> localDeviceName = reader.nextString()
                    LOCAL_DEVICE_ID -> localDeviceId = reader.nextString()
                    DISCOVERY_SERVERS -> {
                        val newDiscoveryServers = HashSet<String>()

                        reader.beginArray()
                        while (reader.hasNext()) {
                            newDiscoveryServers.add(reader.nextString())
                        }
                        reader.endArray()

                        discoveryServers = Collections.unmodifiableSet(newDiscoveryServers)
                    }
                    KEYSTORE_ALGORITHM -> keystoreAlgorithm = reader.nextString()
                    KEYSTORE_DATA -> keystoreData = reader.nextString()
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return Config(
                    peers = peers!!,
                    folders = folders!!,
                    localDeviceName = localDeviceName!!,
                    localDeviceId = localDeviceId!!,
                    discoveryServers = discoveryServers!!,
                    keystoreAlgorithm = keystoreAlgorithm!!,
                    keystoreData = keystoreData!!
            )
        }
    }

    fun serialize(writer: JsonWriter) {
        writer.beginObject()

        writer.name(PEERS).beginArray()
        peers.forEach { it.serialize(writer) }
        writer.endArray()

        writer.name(FOLDERS).beginArray()
        folders.forEach { it.serialize(writer) }
        writer.endArray()

        writer.name(LOCAL_DEVICE_NAME).value(localDeviceName)
        writer.name(LOCAL_DEVICE_ID).value(localDeviceId)

        writer.name(DISCOVERY_SERVERS).beginArray()
        discoveryServers.forEach { writer.value(it) }
        writer.endArray()

        writer.name(KEYSTORE_ALGORITHM).value(keystoreAlgorithm)
        writer.name(KEYSTORE_DATA).value(keystoreData)

        writer.endObject()
    }

    // Exclude keystoreData from toString()
    override fun toString() = "Config(peers=$peers, folders=$folders, localDeviceName=$localDeviceName, " +
            "localDeviceId=$localDeviceId, discoveryServers=$discoveryServers, keystoreAlgorithm=$keystoreAlgorithm)"
}
