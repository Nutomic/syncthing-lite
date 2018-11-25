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

import net.syncthing.java.bep.connectionactor.ConnectionActorWrapper
import java.io.IOException
import java.util.*

class MultiConnectionHelper (
        initialConnections: List<ConnectionActorWrapper>,
        private val connectionFilter: (ConnectionActorWrapper) -> Boolean
) {
    companion object {
        private val random = Random()
    }

    private val usableConnections = initialConnections.toMutableList()

    fun pickConnection(): ConnectionActorWrapper {
        val possibleConnections = synchronized(usableConnections) {
            usableConnections.filter { it.isConnected and connectionFilter(it) }
        }

        if (possibleConnections.isEmpty()) {
            throw IOException("no matching connection is available")
        } else if (possibleConnections.size == 1) {
            return possibleConnections.first()
        } else {
            return possibleConnections[random.nextInt(possibleConnections.size)]
        }
    }

    fun disableConnection(wrapper: ConnectionActorWrapper) {
        synchronized(usableConnections) {
            usableConnections.remove(wrapper)
        }
    }
}
