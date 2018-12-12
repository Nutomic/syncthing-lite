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

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.SendChannel
import net.syncthing.java.bep.BlockExchangeProtos
import java.io.IOException

object ConnectionActorUtil {
    suspend fun waitUntilConnected(actor: SendChannel<ConnectionAction>): ClusterConfigInfo {
        val deferred = CompletableDeferred<ClusterConfigInfo>()

        actor.send(ConfirmIsConnectedAction(deferred))
        actor.invokeOnClose { deferred.cancel() }

        return deferred.await()
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request, actor: SendChannel<ConnectionAction>): BlockExchangeProtos.Response {
        try {
            val deferred = CompletableDeferred<BlockExchangeProtos.Response>()

            actor.send(SendRequestConnectionAction(request, deferred))

            return deferred.await()
        } catch (ex: Exception) {
            throw IOException("not connected", ex)
        }
    }

    suspend fun sendIndexUpdate(update: BlockExchangeProtos.IndexUpdate, actor: SendChannel<ConnectionAction>) {
        try {
            val deferred = CompletableDeferred<Unit?>()

            actor.send(SendIndexUpdateAction(update, deferred))

            deferred.await()
        } catch (ex: Exception) {
            throw IOException("not connected", ex)
        }
    }

    suspend fun disconnect(actor: SendChannel<ConnectionAction>) {
        try {
            actor.send(CloseConnectionAction)
        } catch (ex: Exception) {
            // ignore if the channel is closed already
        }
    }
}
