/* 
 * Copyright (C) 2016 Davide Imbriaco
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

import net.syncthing.java.core.beans.DeviceAddress
import org.slf4j.LoggerFactory
import java.util.*

class DeviceAddressSupplier(private val discoveryHandler: DiscoveryHandler) : Iterable<DeviceAddress?> {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val deviceAddressQueue = PriorityQueue<DeviceAddress>(11, compareBy { it.score })
    private val queueLock = Object()

    private fun getDeviceAddress(): DeviceAddress? {
        synchronized(queueLock) {
            return deviceAddressQueue.poll()
        }
    }

    internal fun onNewDeviceAddressAcquired(address: DeviceAddress) {
        if (address.isWorking()) {
            synchronized(queueLock) {
                deviceAddressQueue.add(address)
                queueLock.notify()
            }
        }
    }

    @Throws(InterruptedException::class)
    fun getDeviceAddressOrWait(): DeviceAddress? = getDeviceAddressOrWait(5000)

    init {
        synchronized(queueLock) {
            deviceAddressQueue.addAll(discoveryHandler.getAllWorkingDeviceAddresses())// note: slight risk of duplicate address loading
        }
    }

    @Throws(InterruptedException::class)
    private fun getDeviceAddressOrWait(timeout: Long): DeviceAddress? {
        synchronized(queueLock) {
            if (deviceAddressQueue.isEmpty()) {
                queueLock.wait(timeout)
            }
            return getDeviceAddress()
        }
    }

    override fun iterator(): Iterator<DeviceAddress?> {
        return object : Iterator<DeviceAddress?> {

            private var hasNext: Boolean? = null
            private var next: DeviceAddress? = null

            override fun hasNext(): Boolean {
                if (hasNext == null) {
                    try {
                        next = getDeviceAddressOrWait()
                    } catch (ex: InterruptedException) {
                        logger.warn("", ex)
                    }

                    hasNext = next != null
                }
                return hasNext!!
            }

            override fun next(): DeviceAddress? {
                assert(hasNext())
                val res = next
                hasNext = null
                next = null
                return res
            }
        }
    }
}
