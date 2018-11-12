package net.syncthing.java.core.configuration

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import net.syncthing.java.core.beans.DeviceId

data class DiscoveryServer(
        val hostname: String,
        val useForLookup: Boolean,
        val useForAnnounce: Boolean,
        val deviceId: DeviceId?
) {
    companion object {
        private const val JSON_HOSTNAME = "host"
        private const val JSON_LOOKUP = "lookup"
        private const val JSON_ANNOUNCE = "announce"
        private const val JSON_DEVICE_ID = "deviceId"

        // from https://github.com/syncthing/syncthing/blob/add12b43aa0bdf5e67d8f421c57a5ecafb3d25fa/lib/config/config.go#L50-L64
        // (if you update it, use the most recent commit)
        private val serverDeviceId = DeviceId("LYXKCHX-VI3NYZR-ALCJBHF-WMZYSPK-QG6QJA3-MPFYMSO-U56GTUK-NA2MIAW")

        private val lookupServer = DiscoveryServer(
                hostname = "discovery.syncthing.net",
                useForLookup = true,
                useForAnnounce = false,
                deviceId = serverDeviceId
        )

        private val announceIpV4Server = DiscoveryServer(
                hostname = "discovery-v4.syncthing.net",
                useForLookup = false,
                useForAnnounce = true,
                deviceId = serverDeviceId
        )

        private val announceIpV6Server = DiscoveryServer(
                hostname = "discovery-v6.syncthing.net",
                useForLookup = false,
                useForAnnounce = true,
                deviceId = serverDeviceId
        )

        val defaultDiscoveryServers = setOf(lookupServer, announceIpV4Server, announceIpV6Server)

        fun parse(reader: JsonReader): DiscoveryServer {
            var hostname: String? = null
            var useForLookup: Boolean? = null
            var useForAnnounce: Boolean? = null
            var deviceId: DeviceId? = null

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    JSON_HOSTNAME -> hostname = reader.nextString()
                    JSON_LOOKUP -> useForLookup = reader.nextBoolean()
                    JSON_ANNOUNCE -> useForAnnounce = reader.nextBoolean()
                    JSON_DEVICE_ID -> deviceId = DeviceId(reader.nextString())
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return DiscoveryServer(
                    hostname = hostname!!,
                    useForLookup = useForLookup!!,
                    useForAnnounce = useForAnnounce!!,
                    deviceId = deviceId
            )
        }
    }

    fun serialize(writer: JsonWriter) {
        writer.beginObject()

        writer.name(JSON_HOSTNAME).value(hostname)
        writer.name(JSON_LOOKUP).value(useForLookup)
        writer.name(JSON_ANNOUNCE).value(useForAnnounce)

        if (deviceId != null) {
            writer.name(JSON_DEVICE_ID).value(deviceId.deviceId)
        }

        writer.endObject()
    }
}
