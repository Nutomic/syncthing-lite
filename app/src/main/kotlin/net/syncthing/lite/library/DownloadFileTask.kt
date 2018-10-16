package net.syncthing.lite.library

import android.os.Handler
import android.os.Looper
import android.support.v4.os.CancellationSignal
import android.util.Log
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.suspendCancellableCoroutine
import net.syncthing.java.bep.BlockPullerStatus
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.BuildConfig
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException

class DownloadFileTask(private val fileStorageDirectory: File,
                       syncthingClient: SyncthingClient,
                       private val fileInfo: FileInfo,
                       private val onProgress: (status: BlockPullerStatus) -> Unit,
                       private val onComplete: (File) -> Unit,
                       private val onError: (Exception) -> Unit) {

    companion object {
        private const val TAG = "DownloadFileTask"
        private val handler = Handler(Looper.getMainLooper())

        suspend fun downloadFileCoroutine(
                externalCacheDir: File,
                syncthingClient: SyncthingClient,
                fileInfo: FileInfo,
                onProgress: (status: BlockPullerStatus) -> Unit
        ) = suspendCancellableCoroutine<File> (holdCancellability = true) {
            continuation ->

            val task = DownloadFileTask(
                    externalCacheDir,
                    syncthingClient,
                    fileInfo,
                    onProgress,
                    {
                        continuation.resume(it)
                    },
                    {
                        continuation.resumeWithException(it)
                    }
            )

            continuation.invokeOnCancellation {
                task.cancel()
            }

            continuation.initCancellability()
        }
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
                        val inputStream = blockPuller.pullFileCoroutine(fileInfo, this@DownloadFileTask::callProgress)

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
                        callError(e)

                        if (BuildConfig.DEBUG) {
                            Log.w(TAG, "Failed to download file $fileInfo", e)
                        }
                    }
                }

                cancellationSignal.setOnCancelListener {
                    job.cancel()
                }
            }, { callError(IOException("could not get block puller for file")) })
        }
    }

    private fun callProgress(status: BlockPullerStatus) {
        handler.post {
            if (!doneListenerCalled) {
                if (BuildConfig.DEBUG) {
                    Log.i("pullFile", "download progress = $status")
                }

                onProgress(status)
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

    private fun callError(exception: Exception) {
        handler.post {
            if (!doneListenerCalled) {
                doneListenerCalled = true

                onError(exception)
            }
        }
    }

    fun cancel() {
        cancellationSignal.cancel()
        callError(InterruptedException())
    }
}
