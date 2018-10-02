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
package net.syncthing.java.core.beans


class IndexInfo private constructor(folder: String, val deviceId: String, val indexId: Long, val localSequence: Long, val maxSequence: Long) : FolderInfo(folder) {

    fun getCompleted(): Double = if (maxSequence > 0) localSequence.toDouble() / maxSequence else 0.0

    init {
        assert(!deviceId.isEmpty())
    }

    fun copyBuilder(): Builder {
        return Builder(folderId, indexId, deviceId, localSequence, maxSequence)
    }

    override fun toString(): String {
        return "FolderIndexInfo{indexId=$indexId, folder=$folderId, deviceId=$deviceId, localSequence=$localSequence, maxSequence=$maxSequence}"
    }

    class Builder {

        private var indexId: Long = 0
        private var deviceId: String? = null
        private var folder: String? = null
        private var localSequence: Long = 0
        private var maxSequence: Long = 0

        internal constructor()

        internal constructor(folder: String, indexId: Long, deviceId: String, localSequence: Long, maxSequence: Long) {
            assert(!folder.isEmpty())
            assert(!deviceId.isEmpty())
            this.folder = folder
            this.indexId = indexId
            this.deviceId = deviceId
            this.localSequence = localSequence
            this.maxSequence = maxSequence
        }

        fun getIndexId(): Long {
            return indexId
        }

        fun getDeviceId(): String? {
            return deviceId
        }

        fun getFolder(): String? {
            return folder
        }

        fun getLocalSequence(): Long {
            return localSequence
        }

        fun getMaxSequence(): Long {
            return maxSequence
        }

        fun setIndexId(indexId: Long): Builder {
            this.indexId = indexId
            return this
        }

        fun setDeviceId(deviceId: String): Builder {
            this.deviceId = deviceId
            return this
        }

        fun setFolder(folder: String): Builder {
            this.folder = folder
            return this
        }

        fun setLocalSequence(localSequence: Long): Builder {
            this.localSequence = localSequence
            return this
        }

        fun setMaxSequence(maxSequence: Long): Builder {
            this.maxSequence = maxSequence
            return this
        }

        fun build(): IndexInfo {
            return IndexInfo(folder!!, deviceId!!, indexId, localSequence, maxSequence)
        }

    }

    companion object {

        fun newBuilder(): Builder {
            return Builder()
        }
    }

}
