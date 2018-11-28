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
package net.syncthing.java.bep

import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.utils.PathUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*

class IndexBrowser internal constructor(private val indexRepository: IndexRepository, private val indexHandler: IndexHandler,
                                        val folder: String, private val includeParentInList: Boolean = false,
                                        private val allowParentInRoot: Boolean = false, ordering: Comparator<FileInfo>?) : Closeable {

    private fun isParent(fileInfo: FileInfo) = PathUtils.isParent(fileInfo.path)

    val ALPHA_ASC_DIR_FIRST: Comparator<FileInfo> =
            compareBy<FileInfo>({!isParent(it)}, {!it.isDirectory()})
                    .thenBy { it.fileName.toLowerCase() }
    val LAST_MOD_DESC: Comparator<FileInfo> =
            compareBy<FileInfo>({!isParent(it)}, {it.lastModified})
                    .thenBy { it.fileName.toLowerCase() }

    private val ordering = ordering ?: ALPHA_ASC_DIR_FIRST
    private val logger = LoggerFactory.getLogger(javaClass)

    var currentPath: String = PathUtils.ROOT_PATH
        private set
    private val PARENT_FILE_INFO: FileInfo
    private val ROOT_FILE_INFO: FileInfo
    private var mOnPathChangedListener: (() -> Unit)? = null

    internal fun onIndexChangedevent(folder: String) {
        if (folder == this.folder) {
            // preloadFileInfoForCurrentPath()
        }
    }

    fun currentPathInfo(): FileInfo = getFileInfoByAbsolutePath(currentPath)

    fun currentPathFileName(): String? = PathUtils.getFileName(currentPath)

    fun isRoot(): Boolean = PathUtils.isRoot(currentPath)

    init {
        assert(folder.isNotEmpty())
        PARENT_FILE_INFO = FileInfo(folder = folder, type = FileInfo.FileType.DIRECTORY, path = PathUtils.PARENT_PATH)
        ROOT_FILE_INFO = FileInfo(folder = folder, type = FileInfo.FileType.DIRECTORY, path = PathUtils.ROOT_PATH)
        navigateToAbsolutePath(PathUtils.ROOT_PATH)
    }

    fun setOnFolderChangedListener(onPathChangedListener: (() -> Unit)?) {
        mOnPathChangedListener = onPathChangedListener
    }

    fun listFiles(path: String = currentPath): List<FileInfo> {
        logger.debug("doListFiles for path = '{}' BEGIN", path)
        val list = ArrayList(indexRepository.runInTransaction { it.findNotDeletedFilesByFolderAndParent(folder, path) })
        logger.debug("doListFiles for path = '{}' : {} records loaded)", path, list.size)
        if (includeParentInList && (!PathUtils.isRoot(path) || allowParentInRoot)) {
            list.add(0, PARENT_FILE_INFO)
        }
        return list.sortedWith(ordering)
    }

    fun getFileInfoByAbsolutePath(path: String): FileInfo {
        return if (PathUtils.isRoot(path)) {
            ROOT_FILE_INFO
        } else {
            logger.debug("doGetFileInfoByAbsolutePath for path = '{}' BEGIN", path)
            val fileInfo = indexRepository.runInTransaction { it.findNotDeletedFileInfo(folder, path) }
                    ?: error("file not found for path = $path")
            logger.debug("doGetFileInfoByAbsolutePath for path = '{}' END", path)
            fileInfo
        }
    }

    fun navigateTo(fileInfo: FileInfo) {
        assert(fileInfo.isDirectory())
        assert(fileInfo.folder == folder)
        return if (fileInfo.path == PARENT_FILE_INFO.path)
            navigateToAbsolutePath(PathUtils.getParentPath(currentPath))
        else
            navigateToAbsolutePath(fileInfo.path)
    }

    fun navigateToNearestPath(oldPath: String) {
        if (!StringUtils.isBlank(oldPath)) {
            navigateToAbsolutePath(oldPath)
        }
    }

    private fun navigateToAbsolutePath(newPath: String) {
        if (PathUtils.isRoot(newPath)) {
            currentPath = PathUtils.ROOT_PATH
        } else {
            val fileInfo = getFileInfoByAbsolutePath(newPath)
            assert(fileInfo.isDirectory(), {"cannot navigate to path ${fileInfo.path}: not a directory"})
            currentPath = fileInfo.path
        }
        logger.info("navigate to path = '{}'", currentPath)
    }

    override fun close() {
        logger.info("closing")
        indexHandler.unregisterIndexBrowser(this)
    }
}
