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

import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import net.syncthing.java.bep.BlockExchangeProtos
import java.io.IOException

class ConnectionActorWrapper (private val source: ReceiveChannel<Pair<SendChannel<ConnectionAction>, ClusterConfigInfo>>) {
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
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request) = ConnectionActorUtil.sendRequest(
            request,
            currentConnectionActor ?: throw IOException("not connected")
    )

    fun hasFolder(folderId: String) = clusterConfigInfo?.getSharedFolders()?.contains(folderId) ?: false

    fun shutdown() {
        job.cancel()
    }
}
