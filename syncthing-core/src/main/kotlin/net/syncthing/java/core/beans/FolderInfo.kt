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

data class FolderInfo(val folderId: String, val label: String) {
    companion object {
        private const val FOLDER_ID = "folderId"
        private const val LABEL = "label"

        fun parse(reader: JsonReader): FolderInfo {
            var folderId: String? = null
            var label: String? = null

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    FOLDER_ID -> folderId = reader.nextString()
                    LABEL -> label = reader.nextString()
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            return FolderInfo(
                    folderId = folderId!!,
                    label = label!!
            )
        }
    }

    init {
        assert(!folderId.isEmpty())
    }

    override fun toString(): String {
        return "FolderInfo(folderId=$folderId, label=$label)"
    }

    fun serialize(writer: JsonWriter) {
        writer.beginObject()

        writer.name(FOLDER_ID).value(folderId)
        writer.name(LABEL).value(label)

        writer.endObject()
    }

}
