package net.syncthing.lite.dialogs.downloadfile

import java.io.Serializable

data class DownloadFileSpec(val folder: String, val path: String, val fileName: String): Serializable
