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
package net.syncthing.java.core.beans

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter

// the whitelist are device ids with which the folder should be shared

// the blacklist are device ids with which the folder should not be shared

// the ignored device ids are devices for which the user confirmed the blacklist entry so that
// there should not be any question
data class FolderInfo(
        val folderId: String,
        val label: String,
        val deviceIdWhitelist: Set<DeviceId>,
        val deviceIdBlacklist: Set<DeviceId>,
        val ignoredDeviceIdList: Set<DeviceId>
) {
    companion object {
        private const val FOLDER_ID = "folderId"
        private const val LABEL = "label"
        private const val DEVICE_ID_WHITELIST = "deviceWhitelist"
        private const val DEVICE_ID_BLACKLIST = "deviceBlacklist"
        private const val IGNORED_DEVICE_ID_LIST = "ignoredDeviceIdList"

        fun parse(reader: JsonReader): FolderInfo {
            var folderId: String? = null
            var label: String? = null
            // the following fields were added later and thus have got a default value
            var deviceIdWhitelist = emptySet<DeviceId>()
            var deviceIdBlacklist = emptySet<DeviceId>()
            var ignoredDeviceIdList = emptySet<DeviceId>()

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    FOLDER_ID -> folderId = reader.nextString()
                    LABEL -> label = reader.nextString()
                    DEVICE_ID_WHITELIST -> {
                        reader.beginArray()

                        deviceIdWhitelist = mutableSetOf<DeviceId>().apply {
                            while (reader.hasNext()) {
                                add(DeviceId(reader.nextString()))
                            }
                        }

                        reader.endArray()
                    }
                    DEVICE_ID_BLACKLIST -> {
                        reader.beginArray()

                        deviceIdBlacklist = mutableSetOf<DeviceId>().apply {
                            while (reader.hasNext()) {
                                add(DeviceId(reader.nextString()))
                            }
                        }

                        reader.endArray()
                    }
                    IGNORED_DEVICE_ID_LIST -> {
                        reader.beginArray()

                        ignoredDeviceIdList = mutableSetOf<DeviceId>().apply {
                            while (reader.hasNext()) {
                                add(DeviceId(reader.nextString()))
                            }
                        }

                        reader.endArray()
                    }
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return FolderInfo(
                    folderId = folderId!!,
                    label = label!!,
                    deviceIdBlacklist = deviceIdBlacklist,
                    deviceIdWhitelist = deviceIdWhitelist,
                    ignoredDeviceIdList = ignoredDeviceIdList
            )
        }
    }

    init {
        assert(!folderId.isEmpty())
        assert(deviceIdWhitelist.find { deviceIdBlacklist.contains(it) } == null)
    }

    val notIgnoredBlacklistEntries: Set<DeviceId> by lazy {
        deviceIdBlacklist
                .filterNot { ignoredDeviceIdList.contains(it) }
                .toSet()
    }

    override fun toString(): String {
        return "FolderInfo(folderId=$folderId, label=$label)"
    }

    fun serialize(writer: JsonWriter) {
        writer.beginObject()

        writer.name(FOLDER_ID).value(folderId)
        writer.name(LABEL).value(label)

        writer.name(DEVICE_ID_WHITELIST).beginArray()
        deviceIdWhitelist.forEach { writer.value(it.deviceId) }
        writer.endArray()

        writer.name(DEVICE_ID_BLACKLIST).beginArray()
        deviceIdBlacklist.forEach { writer.value(it.deviceId) }
        writer.endArray()

        writer.name(IGNORED_DEVICE_ID_LIST).beginArray()
        ignoredDeviceIdList.forEach { writer.value(it.deviceId) }
        writer.endArray()

        writer.endObject()
    }
}
