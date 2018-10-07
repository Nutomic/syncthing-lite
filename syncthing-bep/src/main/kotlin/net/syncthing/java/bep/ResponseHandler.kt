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
package net.syncthing.java.bep

import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.HashMap

class ResponseHandler {
    companion object {
        private val logger = LoggerFactory.getLogger(ResponseHandler::class.java)
    }

    private val responseListeners = Collections.synchronizedMap(HashMap<Int, (BlockExchangeProtos.Response) -> Unit>())
    private val nextRequestId = AtomicInteger(0)

    fun registerListener(listener: (BlockExchangeProtos.Response) -> Unit): Int {
        val requestId = nextRequestId.getAndIncrement()

        responseListeners[requestId] = listener

        return requestId
    }

    fun unregisterListener(requestId: Int) {
        responseListeners.remove(requestId)
    }

    fun handleResponse(response: BlockExchangeProtos.Response) {
        val listener = responseListeners.remove(response.id)

        if (listener != null) {
            listener(response)
        } else {
            logger.warn("received response for {} without associated handler", response.id)
        }
    }
}
