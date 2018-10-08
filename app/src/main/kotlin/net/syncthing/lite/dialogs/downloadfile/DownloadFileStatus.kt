package net.syncthing.lite.dialogs.downloadfile

import java.io.File

sealed class DownloadFileStatus
object DownloadFileStatusFailed: DownloadFileStatus()
data class DownloadFileStatusDone(val file: File): DownloadFileStatus()
data class DownloadFileStatusRunning(val progress: Int): DownloadFileStatus() {
    companion object {
        const val MAX_PROGRESS = 1000
    }
}
