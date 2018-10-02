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

import net.syncthing.java.core.utils.PathUtils
import org.apache.commons.io.FileUtils
import java.util.*

class FileInfo(val folder: String, val type: FileType, val path: String, size: Long? = null,
               lastModified: Date? = Date(0), hash: String? = null, versionList: List<Version>? = null,
               val isDeleted: Boolean = false) {
    val fileName: String
    val parent: String
    val hash: String?
    val size: Long?
    val lastModified: Date
    val versionList: List<Version>

    fun isDirectory(): Boolean = type == FileType.DIRECTORY

    fun isFile(): Boolean = type == FileType.FILE

    init {
        assert(!folder.isEmpty())
        if (PathUtils.isParent(path)) {
            this.fileName = PathUtils.PARENT_PATH
            this.parent = PathUtils.ROOT_PATH
        } else {
            this.fileName = PathUtils.getFileName(path)
            this.parent = if (PathUtils.isRoot(path)) PathUtils.ROOT_PATH else PathUtils.getParentPath(path)
        }
        this.lastModified = lastModified ?: Date(0)
        if (type == FileType.DIRECTORY) {
            this.size = null
            this.hash = null
        } else {
            assert(size != null)
            assert(!hash.isNullOrEmpty())
            this.size = size
            this.hash = hash
        }
        this.versionList = versionList ?: emptyList()
    }

    enum class FileType {
        FILE, DIRECTORY
    }

    fun describeSize(): String = if (isFile()) FileUtils.byteCountToDisplaySize(size!!) else ""

    override fun toString(): String {
        return "FileRecord{" + "folder=" + folder + ", path=" + path + ", size=" + size + ", lastModified=" + lastModified + ", type=" + type + ", last version = " + versionList.lastOrNull() + '}'
    }

    class Version(val id: Long, val value: Long) {

        override fun toString(): String {
            return "Version{id=$id, value=$value}"
        }

    }

    class Builder {

        private var folder: String? = null
        private var path: String? = null
        private var hash: String? = null
        private var size: Long? = null
        private var lastModified = Date(0)
        private var type: FileType? = null
        var versionList: List<Version>? = null
            private set
        private var deleted = false

        fun getFolder(): String? {
            return folder
        }

        fun setFolder(folder: String): Builder {
            this.folder = folder
            return this
        }

        fun getPath(): String? {
            return path
        }

        fun setPath(path: String): Builder {
            this.path = path
            return this
        }

        fun getSize(): Long? {
            return size
        }

        fun setSize(size: Long?): Builder {
            this.size = size
            return this
        }

        fun getLastModified(): Date {
            return lastModified
        }

        fun setLastModified(lastModified: Date): Builder {
            this.lastModified = lastModified
            return this
        }

        fun getType(): FileType? {
            return type
        }

        fun setType(type: FileType): Builder {
            this.type = type
            return this
        }

        fun setTypeFile(): Builder {
            return setType(FileType.FILE)
        }

        fun setTypeDir(): Builder {
            return setType(FileType.DIRECTORY)
        }

        fun setVersionList(versionList: Iterable<Version>?): Builder {
            this.versionList = versionList?.toList()
            return this
        }

        fun isDeleted(): Boolean {
            return deleted
        }

        fun setDeleted(deleted: Boolean): Builder {
            this.deleted = deleted
            return this
        }

        fun getHash(): String? {
            return hash
        }

        fun setHash(hash: String): Builder {
            this.hash = hash
            return this
        }

        fun build(): FileInfo {
            return FileInfo(folder!!, type!!, path!!, size, lastModified, hash, versionList, deleted)
        }

    }

    companion object {

        fun checkBlocks(fileInfo: FileInfo, fileBlocks: FileBlocks) {
            assert(fileBlocks.folder == fileInfo.folder, {"file info folder not match file block folder"})
            assert(fileBlocks.path == fileInfo.path, {"file info path does not match file block path"})
            assert(fileInfo.isFile(), {"file info must be of type 'FILE' to have blocks"})
            assert(fileBlocks.size == fileInfo.size, {"file info size does not match file block size"})
            assert(fileBlocks.hash == fileInfo.hash, {"file info hash does not match file block hash"})
        }
    }

}
