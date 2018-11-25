/*
 * Copyright (C) 2016 Davide Imbriaco
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

import com.google.protobuf.MessageLite
import net.syncthing.java.bep.BlockExchangeProtos

object MessageTypes {
    val messageTypes = listOf(
            MessageTypeInfo(BlockExchangeProtos.MessageType.CLOSE, BlockExchangeProtos.Close::class.java) { BlockExchangeProtos.Close.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.CLUSTER_CONFIG, BlockExchangeProtos.ClusterConfig::class.java) { BlockExchangeProtos.ClusterConfig.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.DOWNLOAD_PROGRESS, BlockExchangeProtos.DownloadProgress::class.java) { BlockExchangeProtos.DownloadProgress.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX, BlockExchangeProtos.Index::class.java) { BlockExchangeProtos.Index.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX_UPDATE, BlockExchangeProtos.IndexUpdate::class.java) { BlockExchangeProtos.IndexUpdate.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.PING, BlockExchangeProtos.Ping::class.java) { BlockExchangeProtos.Ping.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.REQUEST, BlockExchangeProtos.Request::class.java) { BlockExchangeProtos.Request.parseFrom(it) },
            MessageTypeInfo(BlockExchangeProtos.MessageType.RESPONSE, BlockExchangeProtos.Response::class.java) { BlockExchangeProtos.Response.parseFrom(it) }
    )

    val messageTypesByProtoMessageType = messageTypes.map { it.protoMessageType to it }.toMap()
    val messageTypesByJavaClass = messageTypes.map { it.javaClass to it }.toMap()

    fun getIdForMessage(message: MessageLite) = when (message) {
        is BlockExchangeProtos.Request -> Integer.toString(message.id)
        is BlockExchangeProtos.Response -> Integer.toString(message.id)
        else -> Integer.toString(Math.abs(message.hashCode()))
    }
}

data class MessageTypeInfo(
        val protoMessageType: BlockExchangeProtos.MessageType,
        val javaClass: Class<out MessageLite>,
        val parseFrom: (data: ByteArray) -> MessageLite
)
