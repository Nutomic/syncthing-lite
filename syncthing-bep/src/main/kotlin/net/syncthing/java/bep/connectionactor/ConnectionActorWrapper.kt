/*
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
package net.syncthing.java.bep.connectionactor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import java.io.IOException

class ConnectionActorWrapper (
        private val source: ReceiveChannel<Pair<SendChannel<ConnectionAction>, ClusterConfigInfo>>,
        val deviceId: DeviceId,
        val connectivityChangeListener: () -> Unit
) {
    private val job = Job()

    private var currentConnectionActor: SendChannel<ConnectionAction>? = null
    private var clusterConfigInfo: ClusterConfigInfo? = null

    var isConnected = false
        get() = currentConnectionActor?.isClosedForSend == false

    init {
        GlobalScope.launch (job) {
            source.consumeEach { (connectionActor, clusterConfig) ->
                currentConnectionActor = connectionActor
                clusterConfigInfo = clusterConfig
            }
        }

        // this is a very simple solution but it does its job
        GlobalScope.launch (job) {
            var previousConnected = false

            while (isActive) {
                val nowConnected = isConnected

                if (previousConnected != nowConnected) {
                    previousConnected = nowConnected

                    connectivityChangeListener()
                }

                delay(200)
            }
        }
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request) = ConnectionActorUtil.sendRequest(
            request,
            currentConnectionActor ?: throw IOException("not connected")
    )

    suspend fun sendIndexUpdate(update: BlockExchangeProtos.IndexUpdate) = ConnectionActorUtil.sendIndexUpdate(
            update,
            currentConnectionActor ?: throw IOException("not connected")
    )

    fun hasFolder(folderId: String) = clusterConfigInfo?.sharedFolderIds?.contains(folderId) ?: false

    fun getClusterConfig() = clusterConfigInfo ?: throw IOException("not connected")

    fun shutdown() {
        job.cancel()
    }

    // this triggers a disconnection
    // the ConnectionActorGenerator will reconnect soon
    fun reconnect() {
        currentConnectionActor?.close()
    }
}
