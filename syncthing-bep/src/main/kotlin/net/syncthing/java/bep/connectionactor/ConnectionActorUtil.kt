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

import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.channels.SendChannel
import net.syncthing.java.bep.BlockExchangeProtos

object ConnectionActorUtil {
    suspend fun waitUntilConnected(actor: SendChannel<ConnectionAction>) {
        val deferred = CompletableDeferred<Unit?>()

        actor.send(ConfirmIsConnectedAction(deferred))
        actor.invokeOnClose { deferred.cancel() }

        deferred.await()
    }

    suspend fun sendRequest(request: BlockExchangeProtos.Request, actor: SendChannel<ConnectionAction>): BlockExchangeProtos.Response {
        val deferred = CompletableDeferred<BlockExchangeProtos.Response>()

        actor.send(SendRequestConnectionAction(request, deferred))
        actor.invokeOnClose { deferred.cancel() }

        return deferred.await()
    }
}
