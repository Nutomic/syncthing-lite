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

import org.apache.commons.io.FileUtils
import java.util.*

data class FolderStats(val fileCount: Long, val dirCount: Long, val size: Long, val lastUpdate: Date, val folderId: String) {
    companion object {
        fun createDummy(folderId: String) = FolderStats(
                fileCount = 0,
                dirCount = 0,
                size = 0,
                lastUpdate = Date(0),
                folderId = folderId
        )
    }

    val recordCount: Long = dirCount + fileCount

    val sizeDescription: String by lazy { FileUtils.byteCountToDisplaySize(size) }

    val infoDump: String by lazy {
        "folder $folderId file count = $fileCount dir count = $dirCount folder size = $sizeDescription last update = $lastUpdate"
    }
}
