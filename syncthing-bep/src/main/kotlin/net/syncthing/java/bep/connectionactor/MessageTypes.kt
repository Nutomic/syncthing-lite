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

import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.bep.ConnectionHandler

object MessageTypes {
    val messageTypes = listOf(
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.CLOSE, BlockExchangeProtos.Close::class.java) { BlockExchangeProtos.Close.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.CLUSTER_CONFIG, BlockExchangeProtos.ClusterConfig::class.java) { BlockExchangeProtos.ClusterConfig.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.DOWNLOAD_PROGRESS, BlockExchangeProtos.DownloadProgress::class.java) { BlockExchangeProtos.DownloadProgress.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX, BlockExchangeProtos.Index::class.java) { BlockExchangeProtos.Index.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.INDEX_UPDATE, BlockExchangeProtos.IndexUpdate::class.java) { BlockExchangeProtos.IndexUpdate.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.PING, BlockExchangeProtos.Ping::class.java) { BlockExchangeProtos.Ping.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.REQUEST, BlockExchangeProtos.Request::class.java) { BlockExchangeProtos.Request.parseFrom(it) },
            ConnectionHandler.MessageTypeInfo(BlockExchangeProtos.MessageType.RESPONSE, BlockExchangeProtos.Response::class.java) { BlockExchangeProtos.Response.parseFrom(it) }
    )

    val messageTypesByProtoMessageType = messageTypes.map { it.protoMessageType to it }.toMap()
    val messageTypesByJavaClass = messageTypes.map { it.javaClass to it }.toMap()
}
