package net.syncthing.java.core.beans

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import net.syncthing.java.core.utils.NetworkUtils
import org.apache.commons.codec.binary.Base32
import java.io.IOException
import java.io.Serializable

data class DeviceId @Throws(IOException::class) constructor(val deviceId: String): Serializable {

    init {
        val withoutDashes = this.deviceId.replace("-", "")
        NetworkUtils.assertProtocol(DeviceId.fromHashDataToString(toHashData()) == withoutDashes)
    }

    val shortId
        get() = deviceId.substring(0, 7)

    fun toHashData(): ByteArray {
        NetworkUtils.assertProtocol(deviceId.matches("^[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}$".toRegex()), {"device id syntax error for deviceId = $deviceId"})
        val base32data = deviceId.replaceFirst("(.{7})-(.{6}).-(.{7})-(.{6}).-(.{7})-(.{6}).-(.{7})-(.{6}).".toRegex(), "$1$2$3$4$5$6$7$8") + "==="
        val binaryData = Base32().decode(base32data)
        NetworkUtils.assertProtocol(binaryData.size == SHA256_BYTES)
        return binaryData
    }

    companion object {

        private const val DEVICE_ID = "deviceId"

        private const val SHA256_BYTES = 256 / 8

        private fun fromHashDataToString(hashData: ByteArray): String {
            NetworkUtils.assertProtocol(hashData.size == SHA256_BYTES)
            val string = Base32().encodeAsString(hashData).replace("=", "")
            return string.chunked(13).joinToString("") { part -> part + generateLuhn32Checksum(part) }
        }

        fun fromHashData(hashData: ByteArray): DeviceId {
            return DeviceId(fromHashDataToString(hashData).chunked(7).joinToString("-"))
        }

        private fun generateLuhn32Checksum(string: String): Char {
            val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
            var factor = 1
            var sum = 0
            val n = alphabet.length
            for (character in string.toCharArray()) {
                val index = alphabet.indexOf(character)
                NetworkUtils.assertProtocol(index >= 0)
                var add = factor * index
                factor = if (factor == 2) 1 else 2
                add = add / n + add % n
                sum += add
            }
            val remainder = sum % n
            val check = (n - remainder) % n
            return alphabet[check]
        }

        fun parse(reader: JsonReader): DeviceId {
            var deviceId: String? = null

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    DEVICE_ID -> deviceId = reader.nextString()
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return DeviceId(deviceId!!)
        }
    }

    fun serialize(writer: JsonWriter) {
        writer.beginObject()
        writer.name(DEVICE_ID).value(deviceId)
        writer.endObject()
    }
}
