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
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.discovery.protocol.GlobalDiscoveryHandler
import net.syncthing.java.discovery.protocol.LocalDiscoveryHandler
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.io.File
import java.util.concurrent.CountDownLatch

class Main {

    companion object {

        private const val MAX_WAIT = 60 * 1000

        @JvmStatic
        fun main(args: Array<String>) {
            val options = generateOptions()
            val parser = DefaultParser()
            val cmd = parser.parse(options, args)
            if (cmd.hasOption("h")) {
                val formatter = HelpFormatter()
                formatter.printHelp("s-client", options)
                return
            }
            val configuration = if (cmd.hasOption("C")) Configuration(File(cmd.getOptionValue("C")))
            else                                     Configuration()

            val main = Main()
            cmd.options.forEach { main.handleOption(it, configuration) }
        }

        private fun generateOptions(): Options {
            val options = Options()
            options.addOption("C", "set-config", true, "set config file for s-client")
            options.addOption("q", "query", true, "query directory server for device id")
            options.addOption("d", "discovery", true, "discovery local network for device id")
            options.addOption("h", "help", false, "print help")
            return options
        }
    }

    private fun handleOption(option: Option, configuration: Configuration) {
        when (option.opt) {
            "q" -> {
                val deviceId = DeviceId(option.value)
                System.out.println("query device id = $deviceId")
                val latch = CountDownLatch(1)
                GlobalDiscoveryHandler(configuration).query(deviceId, { it ->
                    val addresses = it.map { it.address }.fold("", { l, r -> "$l\n$r"})
                    System.out.println("server response: $addresses")
                    latch.countDown()
                })
                latch.await()
            }
            "d" -> {
                val deviceId = DeviceId(option.value)
                System.out.println("discovery device id = $deviceId")
                val deviceAddresses = queryLocalDiscovery(configuration, deviceId)
                System.out.println("local response = $deviceAddresses")
            }
        }
    }

    private fun queryLocalDiscovery(configuration: Configuration, deviceId: DeviceId): Collection<DeviceAddress> {
        val lock = Object()
        val discoveredAddresses = mutableListOf<DeviceAddress>()
        val handler = LocalDiscoveryHandler(
                configuration,
                {
                    throw it.exception
                },
                { message ->
                    synchronized(lock) {
                        if (message.deviceId == deviceId) {
                            discoveredAddresses.addAll(message.addresses)
                            lock.notify()
                        }
                    }
                }
        )
        handler.startListener()
        handler.sendAnnounceMessage()
        synchronized(lock) {
            try {
                lock.wait(MAX_WAIT.toLong())
            } catch (ex: InterruptedException) {
                System.out.println(ex)
            }
            handler.close()
            return discoveredAddresses
        }
    }

}
