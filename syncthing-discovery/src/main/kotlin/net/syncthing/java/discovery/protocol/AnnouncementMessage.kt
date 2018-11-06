/*
 * Copyright (C) 2016 Davide Imbriaco
 * Copyright (C) 2018 Jonas Lochmann
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
