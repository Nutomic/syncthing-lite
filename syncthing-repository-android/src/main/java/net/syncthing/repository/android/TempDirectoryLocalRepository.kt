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
        deleteAllData()
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
        deleteAllData()
    }

    fun deleteAllData() {
        directory.listFiles().forEach { file ->
            if (file.isFile) {
                file.delete()
            }
        }
    }
}
