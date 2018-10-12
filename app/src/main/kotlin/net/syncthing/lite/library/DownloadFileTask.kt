package net.syncthing.lite.library

import android.os.Handler
import android.os.Looper
import android.support.v4.os.CancellationSignal
import android.util.Log
import kotlinx.coroutines.experimental.launch
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.BuildConfig
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException

class DownloadFileTask(private val fileStorageDirectory: File,
                       syncthingClient: SyncthingClient,
                       private val fileInfo: FileInfo,
                       private val onProgress: (progress: Double, progressMessage: String) -> Unit,
                       private val onComplete: (File) -> Unit,
                       private val onError: () -> Unit) {

    companion object {
        private const val TAG = "DownloadFileTask"
        private val handler = Handler(Looper.getMainLooper())
    }

    private val cancellationSignal = CancellationSignal()
    private var doneListenerCalled = false

    init {
        val file = DownloadFilePath(fileStorageDirectory, fileInfo.hash!!)

        launch {
            if (file.targetFile.exists()) {
                if (BuildConfig.DEBUG) {
                    Log.d(TAG, "there is already a file")
                }

                callComplete(file.targetFile)
            }

            syncthingClient.getBlockPuller(fileInfo.folder, { blockPuller ->
                val job = launch {
                    try {
                        if (!file.filesDirectory.isDirectory) {
                            if (!file.filesDirectory.mkdirs()) {
                                throw IOException("could not create output directory")
                            }
                        }

                        // download the file to a temp location
                        val inputStream = blockPuller.pullFileCoroutine(
                                fileInfo
                        ) { progress, progressMessage -> callProgress(progress, progressMessage) }

                        try {
                            FileUtils.copyInputStreamToFile(inputStream, file.tempFile)
                            file.tempFile.renameTo(file.targetFile)
                        } finally {
                            file.tempFile.delete()
                        }

                        if (BuildConfig.DEBUG) {
                            Log.i(TAG, "Downloaded file $fileInfo")
                        }

                        callComplete(file.targetFile)
                    } catch (e: Exception) {
                        callError()

                        if (BuildConfig.DEBUG) {
                            Log.w(TAG, "Failed to download file $fileInfo", e)
                        }
                    }
                }

                cancellationSignal.setOnCancelListener {
                    job.cancel()
                }
            }, { callError() })
        }
    }

    private fun callProgress(progress: Double, progressMessage: String) {
        handler.post {
            if (!doneListenerCalled) {
                if (BuildConfig.DEBUG) {
                    Log.i("pullFile", "download progress = $progressMessage")
                }

                onProgress(progress, progressMessage)
            }
        }
    }

    private fun callComplete(file: File) {
        handler.post {
            if (!doneListenerCalled) {
                doneListenerCalled = true

                onComplete(file)
            }
        }
    }

    private fun callError() {
        handler.post {
            if (!doneListenerCalled) {
                doneListenerCalled = true

                onError()
            }
        }
    }

    fun cancel() {
        cancellationSignal.cancel()
        callError()
    }
}
