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

import org.apache.commons.io.FileUtils

import java.util.Date

class FolderStats private constructor(val fileCount: Long, val dirCount: Long, val size: Long, val lastUpdate: Date, folder: String, label: String?) : FolderInfo(folder, label) {

    fun getRecordCount(): Long = dirCount + fileCount

    fun describeSize(): String = FileUtils.byteCountToDisplaySize(size)

    fun dumpInfo(): String {
        return ("folder " + label + " (" + folderId + ") file count = " + fileCount
                + " dir count = " + dirCount + " folder size = " + describeSize() + " last update = " + lastUpdate)
    }

    override fun toString(): String {
        return "FolderStats{folder=$folderId, fileCount=$fileCount, dirCount=$dirCount, size=$size, lastUpdate=$lastUpdate}"
    }

    fun copyBuilder(): Builder = Builder(fileCount, dirCount, size, folderId, label)

    class Builder {

        private var fileCount: Long = 0
        private var dirCount: Long = 0
        private var size: Long = 0
        private var lastUpdate = Date(0)
        private var folder: String? = null
        private var label: String? = null

        constructor()

        constructor(fileCount: Long, dirCount: Long, size: Long, folder: String, label: String) {
            this.fileCount = fileCount
            this.dirCount = dirCount
            this.size = size
            this.folder = folder
            this.label = label
        }

        fun getFileCount(): Long = fileCount

        fun setFileCount(fileCount: Long): Builder {
            this.fileCount = fileCount
            return this
        }

        fun getDirCount(): Long = dirCount

        fun setDirCount(dirCount: Long): Builder {
            this.dirCount = dirCount
            return this
        }

        fun getSize(): Long = size

        fun setSize(size: Long): Builder {
            this.size = size
            return this
        }

        fun getLastUpdate(): Date = lastUpdate

        fun setLastUpdate(lastUpdate: Date): Builder {
            this.lastUpdate = lastUpdate
            return this
        }

        fun getFolder(): String? = folder

        fun setFolder(folder: String): Builder {
            this.folder = folder
            return this
        }

        fun getLabel(): String? = label

        fun setLabel(label: String): Builder {
            this.label = label
            return this
        }

        fun build(): FolderStats {
            return FolderStats(fileCount, dirCount, size, lastUpdate, folder!!, label)
        }

    }
}
