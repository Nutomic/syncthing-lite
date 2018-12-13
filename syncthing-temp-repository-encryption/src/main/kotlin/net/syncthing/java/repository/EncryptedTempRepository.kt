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
package net.syncthing.java.repository

import net.syncthing.java.core.interfaces.TempRepository
import java.io.IOException
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class EncryptedTempRepository(private val otherRepository: TempRepository): TempRepository {
    companion object {
        private val secureRandom = SecureRandom()
    }

    private val keyStorage = Collections.synchronizedMap(mutableMapOf<String, EncryptedTempRepositoryItem>())

    override fun pushTempData(data: ByteArray): String {
        val (encrypted, config) = encrypt(data)
        val key = otherRepository.pushTempData(encrypted)

        keyStorage[key] = config

        return key
    }

    override fun popTempData(key: String) = decrypt(otherRepository.popTempData(key), keyStorage.remove(key)!!)

    override fun deleteTempData(keys: List<String>) {
        keys.forEach { keyStorage.remove(it) }
        otherRepository.deleteTempData(keys)
    }

    override fun deleteAllTempData() {
        keyStorage.clear()
        otherRepository.deleteAllTempData()
    }

    override fun close() {
        keyStorage.clear()
        otherRepository.close()
    }

    private fun encrypt(input: ByteArray): Pair<ByteArray, EncryptedTempRepositoryItem> {
        val cryptoSpec = EncryptedTempRepositoryItem(
                key = ByteArray(16).apply { secureRandom.nextBytes(this) },
                iv = ByteArray(16).apply { secureRandom.nextBytes(this) },
                sha512 = sha512(input)
        )

        val output = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(
                    Cipher.ENCRYPT_MODE,
                    SecretKeySpec(cryptoSpec.key, "AES"),
                    GCMParameterSpec(128, cryptoSpec.iv)
            )
        }.doFinal(input)

        return output to cryptoSpec
    }

    private fun decrypt(input: ByteArray, config: EncryptedTempRepositoryItem): ByteArray {
        val output = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(
                    Cipher.DECRYPT_MODE,
                    SecretKeySpec(config.key, "AES"),
                    GCMParameterSpec(128, config.iv)
            )
        }.doFinal(input)

        if (!sha512(output).contentEquals(config.sha512)) {
            throw IOException("temporarily file was modified")
        }

        return output
    }

    private fun sha512(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-512").digest(data)
}
