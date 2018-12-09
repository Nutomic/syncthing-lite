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
package net.syncthing.java.bep.folder

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.first
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.syncthing.java.bep.index.*
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.java.core.configuration.Configuration
import java.io.Closeable

class FolderBrowser internal constructor(private val indexHandler: IndexHandler, private val configuration: Configuration) : Closeable {
    private val job = Job()
    private val foldersStatus = ConflatedBroadcastChannel<Map<String, FolderStatus>>()

    init {
        GlobalScope.launch (job) {
            // get initial status
            val currentFolderStats = mutableMapOf<String, FolderStats>()

            val currentIndexInfo = withContext(Dispatchers.IO) {
                indexHandler.indexRepository.runInTransaction { indexTransaction ->
                    configuration.folders.map { it.folderId }.forEach { folderId ->
                        currentFolderStats[folderId] = indexTransaction.findFolderStats(folderId) ?: FolderStats.createDummy(folderId)
                    }

                    indexTransaction.findAllIndexInfos().groupBy { it.folderId }.toMutableMap()
                }
            }

            // send status
            suspend fun dispatch() {
                foldersStatus.send(
                        configuration.folders.map { info ->
                            FolderStatus(
                                    info = info,
                                    stats = currentFolderStats[info.folderId] ?: FolderStats.createDummy(info.folderId),
                                    indexInfo = currentIndexInfo[info.folderId] ?: emptyList()
                            )
                        }.associateBy { it.info.folderId }
                )
            }

            dispatch()

            // handle changes
            val updateLock = Mutex()

            async {
                indexHandler.subscribeFolderStatsUpdatedEvents().consumeEach { event ->
                    updateLock.withLock {
                        when (event) {
                            is FolderStatsUpdatedEvent -> currentFolderStats[event.folderStats.folderId] = event.folderStats
                            FolderStatsResetEvent -> currentFolderStats.clear()
                        }.let { /* require that all cases are handled */ }

                        dispatch()
                    }
                }
            }

            async {
                indexHandler.subscribeToOnIndexUpdateEvents().consumeEach { event ->
                    updateLock.withLock {
                        when (event) {
                            is IndexRecordAcquiredEvent -> {
                                val oldList = currentIndexInfo[event.folderId] ?: emptyList()
                                val newList = oldList.filter { it.deviceId != event.indexInfo.deviceId } + event.indexInfo

                                currentIndexInfo[event.folderId] = newList
                            }
                            IndexInfoClearedEvent -> currentIndexInfo.clear()
                        }.let { /* require that all cases are handled */ }

                        dispatch()
                    }
                }
            }

            async {
                configuration.subscribe().consumeEach {
                    dispatch()
                }
            }
        }
    }

    fun folderInfoAndStatusStream() = GlobalScope.produce {
        foldersStatus.openSubscription().consumeEach { folderStats ->
            send(
                    folderStats
                            .values
                            .sortedBy { it.info.label }
            )
        }
    }

    suspend fun folderInfoAndStatusList(): List<FolderStatus> = folderInfoAndStatusStream().first()

    suspend fun getFolderStatus(folder: String): FolderStatus {
        return getFolderStatus(folder, foldersStatus.openSubscription().first())
    }

    fun getFolderStatusSync(folder: String) = runBlocking { getFolderStatus(folder) }

    private fun getFolderStatus(folder: String, folderStatus: Map<String, FolderStatus>) = folderStatus[folder] ?: FolderStatus.createDummy(folder)

    override fun close() {
        job.cancel()
    }
}
