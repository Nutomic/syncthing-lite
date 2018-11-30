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
package net.syncthing.java.core.interfaces

import net.syncthing.java.core.beans.*
import java.util.*

interface IndexTransaction {
    fun getSequencer(): Sequencer

    fun updateIndexInfo(indexInfo: IndexInfo)

    fun findAllIndexInfos(): List<IndexInfo>

    fun findIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): IndexInfo?

    fun findFileInfo(folder: String, path: String): FileInfo?

    // path to FileInfo
    fun findFileInfo(folder: String, path: List<String>): Map<String, FileInfo>

    fun findFileInfoLastModified(folder: String, path: String): Date?

    fun findNotDeletedFileInfo(folder: String, path: String): FileInfo?

    fun findFileBlocks(folder: String, path: String): FileBlocks?

    fun updateFileInfo(fileInfo: FileInfo, fileBlocks: FileBlocks?)

    fun updateFileInfoAndBlocks(fileInfos: List<FileInfo>, fileBlocks: List<FileBlocks>)

    fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String): List<FileInfo>

    fun clearIndex()

    fun findFolderStats(folder: String): FolderStats?

    fun findAllFolderStats(): List<FolderStats>

    fun updateOrInsertFolderStats(folder: String, deltaFileCount: Long, deltaDirCount: Long, deltaSize: Long, lastUpdate: Date)

    fun findFileInfoBySearchTerm(query: String): List<FileInfo>

    fun countFileInfoBySearchTerm(query: String): Long
}
