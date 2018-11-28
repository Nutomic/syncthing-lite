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
package net.syncthing.java.bep

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import net.syncthing.java.bep.index.IndexHandler
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.interfaces.IndexRepository
import java.io.Closeable

class FolderBrowser internal constructor(private val indexHandler: IndexHandler, private val configuration: Configuration) : Closeable {
    private val job = Job()
    private val folderStats = ConflatedBroadcastChannel<Map<String, FolderStats>>()
    private val folderStatsUpdatedEvent = Channel<IndexRepository.FolderStatsUpdatedEvent>(capacity = Channel.UNLIMITED)

    // FIXME: This isn't nice
    private val indexRepositoryEventListener = { event: IndexRepository.FolderStatsUpdatedEvent ->
        folderStatsUpdatedEvent.offer(event)

        fun nothing() {
            // used to return Unit
        }

        nothing()
    }

    init {
        indexHandler.indexRepository.setOnFolderStatsUpdatedListener(indexRepositoryEventListener)  // TODO: remove this global state

        GlobalScope.launch (job) {
            // get initial status
            val currentFolderStats = mutableMapOf<String, FolderStats>()

            indexHandler.indexRepository.runInTransaction { indexTransaction ->
                configuration.folders.map { it.folderId }.forEach { folderId ->
                    currentFolderStats[folderId] = indexTransaction.findFolderStats(folderId) ?: FolderStats.createDummy(folderId)
                }
            }

            folderStats.send(currentFolderStats.toMap())

            // handle changes
            folderStatsUpdatedEvent.consumeEach {
                it.getFolderStats().forEach { folderStats ->
                    currentFolderStats[folderStats.folderId] = folderStats
                }

                folderStats.send(currentFolderStats.toMap())
            }
        }
    }

    fun folderInfoAndStatsStream() = GlobalScope.produce {
        folderStats.openSubscription().consumeEach { folderStats ->
            send(
                    configuration.folders
                            .map { folderInfo -> folderInfo to getFolderStats(folderInfo.folderId, folderStats) }
                            .sortedBy { it.first.label }
            )
        }
    }

    suspend fun folderInfoAndStatsList(): List<Pair<FolderInfo, FolderStats>> = folderInfoAndStatsStream().first()

    suspend fun getFolderStats(folder: String): FolderStats {
        return getFolderStats(folder, folderStats.openSubscription().first())
    }

    fun getFolderStatsSync(folder: String) = runBlocking { getFolderStats(folder) }

    private fun getFolderStats(folder: String, folderStats: Map<String, FolderStats>) = folderStats[folder] ?: FolderStats.createDummy(folder)

    override fun close() {
        job.cancel()
        indexHandler.indexRepository.setOnFolderStatsUpdatedListener(null)
        folderStatsUpdatedEvent.close()
    }
}
