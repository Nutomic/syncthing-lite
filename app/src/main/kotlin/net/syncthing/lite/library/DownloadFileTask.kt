package net.syncthing.lite.library

import android.os.Handler
import android.os.Looper
import android.util.Log
import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.BuildConfig
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException

class DownloadFileTask(private val externalCacheDir: File,
                       syncthingClient: SyncthingClient,
                       private val fileInfo: FileInfo,
                       private val onProgress: (DownloadFileTask, BlockPuller.FileDownloadObserver) -> Unit,
                       private val onComplete: (File) -> Unit,
                       private val onError: () -> Unit) {

    companion object {
        private const val TAG = "DownloadFileTask"
        private val handler = Handler(Looper.getMainLooper())
    }

    private var isCancelled = false
    private var doneListenerCalled = false

    init {
        syncthingClient.getBlockPuller(fileInfo.folder, { blockPuller ->
            val observer = blockPuller.pullFile(fileInfo)

            callProgress(observer)

            try {
                while (!observer.isCompleted()) {
                    if (isCancelled) {
                        callError()
                        return@getBlockPuller
                    }

                    observer.waitForProgressUpdate()

                    if (BuildConfig.DEBUG) {
                        Log.i("pullFile", "download progress = " + observer.progressMessage())
                    }

                    callProgress(observer)
                }

                val outputFile = File("$externalCacheDir/${fileInfo.folder}/${fileInfo.path}")
                FileUtils.copyInputStreamToFile(observer.inputStream(), outputFile)

                if (BuildConfig.DEBUG) {
                    Log.i(TAG, "Downloaded file $fileInfo")
                }

                callComplete(outputFile)
            } catch (e: IOException) {
                callError()

                if (BuildConfig.DEBUG) {
                    Log.w(TAG, "Failed to download file $fileInfo", e)
                }
            }
        }, { callError() })
    }

    private fun callProgress(observer: BlockPuller.FileDownloadObserver) {
        handler.post {
            if (!doneListenerCalled) {
                onProgress(this, observer)
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
        isCancelled = true
        callError()
    }
}
