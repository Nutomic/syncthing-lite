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
package net.syncthing.repository.android

import net.syncthing.java.core.interfaces.TempRepository
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.*

// the old implementation used a database for it, but I think that the filesystem is better for this
// as it would theoretically allow streaming
class TempDirectoryLocalRepository(private val directory: File): TempRepository {
    init {
        // create the temp directory if it does not exist
        directory.mkdirs()

        // there could be garbage from the previous session which we don't need anymore
        deleteAllTempData()
    }

    override fun pushTempData(data: ByteArray): String {
        // generate a random key like the old implementation
        val key = UUID.randomUUID().toString()

        val file = File(directory, key)

        FileOutputStream(file).use { outputStream ->
            outputStream.write(data)
        }

        return key
    }

    override fun popTempData(key: String): ByteArray {
        val file = File(directory, key)
        val size = file.length()
        val buffer = ByteArray(size.toInt())

        FileInputStream(file).use { inputStream ->
            val bytesRead = inputStream.read(buffer)

            // assert that the buffer is full
            if (bytesRead != size.toInt()) {
                throw IllegalStateException()
            }

            // assert that there is not more in the stream
            if (inputStream.read(buffer) >= 0) {
                throw IllegalStateException()
            }
        }

        return buffer
    }

    override fun deleteTempData(keys: List<String>) {
        keys.forEach {
            key -> File(directory, key).delete()
        }
    }

    override fun close() {
        deleteAllTempData()
    }

    override fun deleteAllTempData() {
        directory.listFiles().forEach { file ->
            if (file.isFile) {
                file.delete()
            }
        }
    }
}
