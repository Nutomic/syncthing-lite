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
package net.syncthing.java.core.beans

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import java.util.*

// TODO: this should use a data class, but the custom equals prevents it
class DeviceAddress private constructor(val deviceId: DeviceId, private val instanceId: Long?, val address: String, producer: AddressProducer?, score: Int?, lastModified: Date?) {
    private val producer = producer ?: AddressProducer.UNKNOWN
    val score = score ?: Integer.MAX_VALUE
    private val lastModified = lastModified ?: Date()

    @Throws(UnknownHostException::class)
    private fun getInetAddress(): InetAddress = InetAddress.getByName(address.replaceFirst("^[^:]+://".toRegex(), "").replaceFirst("(:[0-9]+)?(/.*)?$".toRegex(), ""))

    private val port: Int by lazy {
        if (address.matches("^[a-z]+://[^:]+:([0-9]+).*".toRegex())) {
            Integer.parseInt(address.replaceFirst("^[a-z]+://[^:]+:([0-9]+).*".toRegex(), "$1"))
        } else {
            DEFAULT_PORT_BY_PROTOCOL[type]!!
        }
    }

    val type: AddressType by lazy {
        when {
            address.isEmpty() -> AddressType.NULL
            address.startsWith("tcp://") -> AddressType.TCP
            address.startsWith("relay://") -> AddressType.RELAY
            else -> AddressType.OTHER
        }
    }

    @Throws(UnknownHostException::class)
    fun getSocketAddress(): InetSocketAddress = InetSocketAddress(getInetAddress(), port)

    fun isWorking(): Boolean = score < Integer.MAX_VALUE

    constructor(deviceId: String, address: String) : this(DeviceId(deviceId), null, address, null, null, null)

    fun containsUriParamValue(key: String): Boolean {
        return !getUriParam(key).isNullOrEmpty()
    }

    /**
     * Returns value for the specified URL parameter key.
     *
     * We need to parse the URL manually, as it is not URL encoded and may contain invalid key/values
     * like "key=a b" (with an unencoded space).
     */
    fun getUriParam(key: String): String? {
        assert(!key.isEmpty())
        return address
                .split("?", limit = 2).first()
                .splitToSequence("&")
                .map { it.split("=", limit = 2) }
                .map { it[0] to (it.getOrNull(1) ?: "") }
                .find { it.first == key }
                ?.second
    }

    enum class AddressType {
        TCP, RELAY, OTHER, NULL
    }

    enum class AddressProducer {
        LOCAL_DISCOVERY, GLOBAL_DISCOVERY, UNKNOWN
    }

    override fun toString(): String {
        return "DeviceAddress(deviceId=$deviceId, instanceId=$instanceId, address=$address, producer=$producer, score=$score, lastModified=$lastModified)"
    }

    override fun hashCode(): Int {
        var hash = 3
        hash = 29 * hash + Objects.hashCode(this.deviceId)
        hash = 29 * hash + Objects.hashCode(this.address)
        return hash
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null) {
            return false
        }
        if (javaClass != other.javaClass) {
            return false
        }
        other as DeviceAddress
        if (this.deviceId != other.deviceId) {
            return false
        }
        return this.address == other.address
    }

    fun copyBuilder(): Builder {
        return Builder(deviceId, instanceId, address, producer, score, lastModified)
    }

    class Builder {

        private var deviceId: DeviceId? = null
        private var instanceId: Long? = null
        private var address: String? = null
        private var producer: AddressProducer? = null
        private var score: Int? = null
        private var lastModified: Date? = null

        constructor()

        internal constructor(deviceId: DeviceId, instanceId: Long?, address: String, producer: AddressProducer, score: Int?, lastModified: Date) {
            this.deviceId = deviceId
            this.instanceId = instanceId
            this.address = address
            this.producer = producer
            this.score = score
            this.lastModified = lastModified
        }

        fun getLastModified(): Date? {
            return lastModified
        }

        fun setLastModified(lastModified: Date): Builder {
            this.lastModified = lastModified
            return this
        }

        fun getDeviceId(): DeviceId? {
            return deviceId
        }

        fun setDeviceId(deviceId: DeviceId): Builder {
            this.deviceId = deviceId
            return this
        }

        fun getInstanceId(): Long? {
            return instanceId
        }

        fun setInstanceId(instanceId: Long?): Builder {
            this.instanceId = instanceId
            return this
        }

        fun getAddress(): String? {
            return address
        }

        fun setAddress(address: String): Builder {
            this.address = address
            return this
        }

        fun getProducer(): AddressProducer? {
            return producer
        }

        fun setProducer(producer: AddressProducer): Builder {
            this.producer = producer
            return this
        }

        fun getScore(): Int? {
            return score
        }

        fun setScore(score: Int?): Builder {
            this.score = score
            return this
        }

        fun build(): DeviceAddress {
            return DeviceAddress(deviceId!!, instanceId, address!!, producer, score, lastModified)
        }
    }

    companion object {
        private val DEFAULT_PORT_BY_PROTOCOL = mapOf(
                AddressType.TCP to 22000,
                AddressType.RELAY to 22067
        )
    }
}
