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
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.DeviceId
import java.io.IOException

class ConnectionActorWrapper (
        private val source: ReceiveChannel<Pair<Connection, ConnectionInfo>>,
        val deviceId: DeviceId
) {
    private val job = Job()

    private var connection: Connection? = null
    private val connectionInfo = ConflatedBroadcastChannel<ConnectionInfo>(ConnectionInfo.empty)

    val isConnected
        get() = connectionInfo.valueOrNull?.status == ConnectionStatus.Connected

    init {
        GlobalScope.launch (job) {
            source.consumeEach { (connection, connectionInfo) ->
                this@ConnectionActorWrapper.connection = connection
                this@ConnectionActorWrapper.connectionInfo.send(connectionInfo)
            }
        }
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request) = ConnectionActorUtil.sendRequest(
            request,
            connection?.actor ?: throw IOException("not connected")
    )

    suspend fun sendIndexUpdate(update: BlockExchangeProtos.IndexUpdate) = ConnectionActorUtil.sendIndexUpdate(
            update,
            connection?.actor ?: throw IOException("not connected")
    )

    fun hasFolder(folderId: String) = connection?.clusterConfigInfo?.sharedFolderIds?.contains(folderId) ?: false

    fun getClusterConfig() = connection?.clusterConfigInfo ?: throw IOException("not connected")

    fun shutdown() {
        job.cancel()
        connectionInfo.close()
    }

    // this triggers a disconnection
    // the ConnectionActorGenerator will reconnect soon
    fun reconnect() {
        val actor = connection?.actor

        GlobalScope.launch {
            if (actor != null) {
                ConnectionActorUtil.disconnect(actor)
            }
        }
    }

    fun subscribeToConnectionInfo() = connectionInfo.openSubscription()
}
