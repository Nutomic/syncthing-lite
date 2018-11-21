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
package net.syncthing.java.discovery

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId

class DeviceAddressesManager (val deviceId: DeviceId) {
    private val lock = Object()
    private val deviceAddressesCache = mutableListOf<DeviceAddress>()
    private val listeners = mutableListOf<(DeviceAddress) -> Unit>()

    fun putAddress(address: DeviceAddress) {
        if (address.deviceId != deviceId) {
            throw IllegalArgumentException()
        }

        synchronized(lock) {
            deviceAddressesCache.add(address)
            listeners.forEach { it(address) }
        }
    }

    private fun addListener(listener: (DeviceAddress) -> Unit) {
        synchronized(lock) {
            listeners.add(listener)
        }
    }

    private fun removeListener(listener: (DeviceAddress) -> Unit) {
        synchronized(lock) {
            listeners.remove(listener)
        }
    }

    // this creates a copy of the set
    fun getCurrentDeviceAddresses() = synchronized(lock) {
        deviceAddressesCache.toList()
    }

    fun streamCurrentDeviceAddresses(): ReceiveChannel<DeviceAddress> = Channel<DeviceAddress>(capacity = Channel.UNLIMITED).apply {
        val listener: (DeviceAddress) -> Unit = {
            offer(it)
        }

        invokeOnClose {
            removeListener(listener)
        }

        synchronized(lock) {
            addListener(listener)
            deviceAddressesCache.forEach { listener(it) }
        }
    }
}
