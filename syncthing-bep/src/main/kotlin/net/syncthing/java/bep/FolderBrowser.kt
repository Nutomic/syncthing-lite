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

import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.java.core.interfaces.IndexRepository
import java.io.Closeable

class FolderBrowser internal constructor(private val indexHandler: IndexHandler) : Closeable {
    private val folderStatsCache = mutableMapOf<String, FolderStats>()
    private val indexRepositoryEventListener = { event: IndexRepository.FolderStatsUpdatedEvent ->
        addFolderStats(event.getFolderStats())
    }

    fun folderInfoAndStatsList(): List<Pair<FolderInfo, FolderStats>> =
            indexHandler.folderInfoList()
                    .map { folderInfo -> Pair(folderInfo, getFolderStats(folderInfo.folderId)) }
                    .sortedBy { it.first.label }

    init {
        indexHandler.indexRepository.setOnFolderStatsUpdatedListener(indexRepositoryEventListener)
        addFolderStats(indexHandler.indexRepository.findAllFolderStats())
    }

    private fun addFolderStats(folderStatsList: List<FolderStats>) {
        for (folderStats in folderStatsList) {
            folderStatsCache.put(folderStats.folderId, folderStats)
        }
    }

    fun getFolderStats(folder: String): FolderStats {
        return folderStatsCache[folder] ?: let {
            FolderStats.Builder()
                    .setFolder(folder)
                    .build()
        }
    }

    fun getFolderInfo(folder: String): FolderInfo? {
        return indexHandler.getFolderInfo(folder)
    }

    override fun close() {
        indexHandler.indexRepository.setOnFolderStatsUpdatedListener(null)
    }
}
