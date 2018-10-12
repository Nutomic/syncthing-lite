package net.syncthing.lite.library

import java.io.File

data class DownloadFilePath (val baseDirectory: File, val fileHash: String) {
    val filesDirectory = File(baseDirectory, fileHash.substring(0, 2))
    val targetFile = File(filesDirectory, fileHash.substring(2))
    val tempFile = File(filesDirectory, fileHash.substring(2) + "_temp")
}
