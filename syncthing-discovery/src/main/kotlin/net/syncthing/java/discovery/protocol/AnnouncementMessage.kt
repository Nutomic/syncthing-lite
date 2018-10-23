package net.syncthing.java.discovery.protocol

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.util.*

data class AnnouncementMessage(val addresses: List<String>) {
    companion object {
        private const val ADDRESSES = "addresses"

        fun parse(reader: JsonReader): AnnouncementMessage {
            var addresses = listOf<String>()

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    ADDRESSES -> {
                        val newAddresses = ArrayList<String>()

                        if (reader.peek() == JsonToken.NULL) {
                            reader.skipValue()
                        } else {
                            reader.beginArray()
                            while (reader.hasNext()) {
                                newAddresses.add(reader.nextString())
                            }
                            reader.endArray()
                        }

                        addresses = Collections.unmodifiableList(newAddresses)
                    }
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return AnnouncementMessage(addresses)
        }
    }
}
