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
package net.syncthing.java.client

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import net.syncthing.java.bep.connectionactor.ConnectionInfo
import net.syncthing.java.core.beans.DeviceId

class Connections (val generate: (DeviceId) -> ConnectionActorWrapper) {
    private val map = mutableMapOf<DeviceId, ConnectionActorWrapper>()
    private val connectionStatus = ConflatedBroadcastChannel<Map<DeviceId, ConnectionInfo>>(emptyMap())
    private val connectionStatusLock = Mutex()
    private val job = Job()

    fun getByDeviceId(deviceId: DeviceId): ConnectionActorWrapper {
        synchronized(map) {
            val oldEntry = map[deviceId]

            if (oldEntry != null) {
                return oldEntry
            } else {
                val newEntry = generate(deviceId)

                map[deviceId] = newEntry

                GlobalScope.launch (job) {
                    newEntry.subscribeToConnectionInfo().consumeEach {  status ->
                        connectionStatusLock.withLock {
                            connectionStatus.send(
                                    connectionStatus.value +
                                            mapOf(deviceId to status)
                            )
                        }
                    }
                }

                return newEntry
            }
        }
    }

    fun shutdown() {
        synchronized(map) {
            map.values.forEach { it.shutdown() }
        }

        job.cancel()
    }

    fun reconnectAllConnections() {
        synchronized(map) {
            map.values.forEach { it.reconnect() }
        }
    }

    fun subscribeToConnectionStatusMap() = connectionStatus.openSubscription()
}
