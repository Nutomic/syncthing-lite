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
package net.syncthing.java.bep.index.browser

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.consume
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.IndexTransaction
import net.syncthing.java.core.utils.PathUtils
import java.util.*

class IndexBrowser internal constructor(
        private val indexRepository: IndexRepository,
        private val indexHandler: IndexHandler
) {
    companion object {
        val sortAlphabeticallyDirectoriesFirst: Comparator<FileInfo> =
                compareBy<FileInfo>({!isParent(it) }, {!it.isDirectory()})
                        .thenBy { it.fileName.toLowerCase() }

        val sortByLastModification: Comparator<FileInfo> =
                compareBy<FileInfo>({!isParent(it) }, {it.lastModified})
                        .thenBy { it.fileName.toLowerCase() }

        private fun isParent(fileInfo: FileInfo) = PathUtils.isParent(fileInfo.path)

        fun getPathFileName(path: String) = PathUtils.getFileName(path)

        const val ROOT_PATH = PathUtils.ROOT_PATH
    }

    fun getDirectoryListing(folder: String, path: String): DirectoryListing = indexRepository.runInTransaction { indexTransaction ->
        val entries = indexTransaction.findNotDeletedFilesByFolderAndParent(folder, path)
        val parentPath = if (PathUtils.isRoot(path)) null else PathUtils.getParentPath(path)
        val parentEntry = if (PathUtils.isRoot(path)) null else getFileInfoByPathAllowNull(folder, PathUtils.getParentPath(path), indexTransaction)
        val directoryInfo = getFileInfoByPathAllowNull(folder, path, indexTransaction)

        if ((parentPath != null && parentEntry == null) || directoryInfo == null || directoryInfo.type != FileInfo.FileType.DIRECTORY) {
            DirectoryNotFoundListing(folder, path)
        } else {
            DirectoryContentListing(
                    entries = entries,
                    parentEntry = parentEntry,
                    directoryInfo = directoryInfo
            )
        }
    }

    fun streamDirectoryListing(folder: String, path: String) = GlobalScope.produce {
        indexHandler.subscribeToOnIndexRecordAcquiredEvents().consume {
            val directoryName = PathUtils.getFileName(path)
            val parentPath = if (PathUtils.isRoot(path)) null else PathUtils.getParentPath(path)
            val parentDirectoryName = if (parentPath != null) PathUtils.getFileName(parentPath) else null
            val parentParentPath = if (parentPath == null || PathUtils.isRoot(parentPath)) null else PathUtils.getParentPath(parentPath)

            // get the initial state
            var (entries, parentEntry, directoryInfo) = withContext (Dispatchers.IO) {
                indexRepository.runInTransaction { indexTransaction ->
                    val entries = indexTransaction.findNotDeletedFilesByFolderAndParent(folder, path)
                    val parentEntry = if (PathUtils.isRoot(path)) null else getFileInfoByPathAllowNull(folder, PathUtils.getParentPath(path), indexTransaction)
                    val directoryInfo = getFileInfoByPathAllowNull(folder, path, indexTransaction)

                    Triple(entries, parentEntry, directoryInfo)
                }
            }

            var previousStatus: DirectoryListing? = null

            suspend fun dispatch() {
                // let Kotlin understand that the value does not change during running this
                val directoryInfo = directoryInfo

                val newStatus = if ((parentPath != null && parentEntry == null) || directoryInfo == null || directoryInfo.type != FileInfo.FileType.DIRECTORY) {
                    DirectoryNotFoundListing(folder, path)
                } else {
                    DirectoryContentListing(
                            entries = entries,
                            parentEntry = parentEntry,
                            directoryInfo = directoryInfo
                    )
                }

                if (newStatus != previousStatus) {
                    previousStatus = newStatus
                    send(newStatus)
                }
            }

            dispatch()

            // handle updates
            for (event in this) {
                var hadChanges = false

                if (event.folderId == folder) {
                    event.files.forEach { fileUpdate ->
                        // entry change
                        if (fileUpdate.parent == path) {
                            hadChanges = true

                            entries = entries.filter { it.fileName != fileUpdate.fileName }

                            if (!fileUpdate.isDeleted) {
                                entries += listOf(fileUpdate)
                            }
                        }

                        // handle directory info changes
                        if (fileUpdate.parent == parentPath && fileUpdate.fileName == directoryName) {
                            directoryInfo = if (fileUpdate.isDeleted) null else fileUpdate
                            hadChanges = true
                        }

                        // handle parent directory info changes
                        if (fileUpdate.parent == parentParentPath && fileUpdate.fileName == parentDirectoryName) {
                            parentEntry = if (fileUpdate.isDeleted) null else fileUpdate
                            hadChanges = true
                        }
                    }
                }

                if (hadChanges) {
                    dispatch()
                }
            }
        }
    }

    fun getFileInfoByAbsolutePath(folder: String, path: String): FileInfo = getFileInfoByAbsolutePathAllowNull(folder, path)
            ?: error("file not found for path = $path")

    fun getFileInfoByAbsolutePathAllowNull(folder: String, path: String): FileInfo? {
        return if (PathUtils.isRoot(path)) {
            FileInfo(folder = folder, type = FileInfo.FileType.DIRECTORY, path = PathUtils.ROOT_PATH)
        } else {
            indexRepository.runInTransaction { it.findNotDeletedFileInfo(folder, path) }
        }
    }

    fun getFileInfoByPath(folder: String, path: String, transaction: IndexTransaction) = getFileInfoByPathAllowNull(folder, path, transaction)
            ?: error("file not found for path = $path")

    fun getFileInfoByPathAllowNull(folder: String, path: String, transaction: IndexTransaction): FileInfo? {
        return if (PathUtils.isRoot(path)) {
            FileInfo(folder = folder, type = FileInfo.FileType.DIRECTORY, path = PathUtils.ROOT_PATH)
        } else {
            transaction.findNotDeletedFileInfo(folder, path)
        }
    }
}
