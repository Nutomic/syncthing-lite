package net.syncthing.lite.library

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FileInfo
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException

class DownloadFileTask(private val context: Context, syncthingClient: SyncthingClient,
                       private val fileInfo: FileInfo,
                       private val onProgress: (DownloadFileTask, BlockPuller.FileDownloadObserver) -> Unit,
                       private val onComplete: (File) -> Unit,
                       private val onError: () -> Unit) {

    companion object {
        private const val TAG = "DownloadFileTask"
        private val handler = Handler(Looper.getMainLooper())
    }

    private var isCancelled = false

    init {
        syncthingClient.getBlockPuller(fileInfo.folder, { blockPuller ->
            val observer = blockPuller.pullFile(fileInfo)

            handler.post { onProgress(this, observer) }

            try {
                while (!observer.isCompleted()) {
                    if (isCancelled)
                        return@getBlockPuller

                    observer.waitForProgressUpdate()
                    Log.i("pullFile", "download progress = " + observer.progressMessage())
                    handler.post { onProgress(this, observer) }
                }

                val outputFile = File("${context.externalCacheDir}/${fileInfo.folder}/${fileInfo.path}")
                FileUtils.copyInputStreamToFile(observer.inputStream(), outputFile)
                Log.i(TAG, "Downloaded file $fileInfo")
                handler.post { onComplete(outputFile) }
            } catch (e: IOException) {
                handler.post { onError() }
                Log.w(TAG, "Failed to download file $fileInfo", e)
            }
        }, { handler.post { onError() } })
    }

    fun cancel() {
        isCancelled = true
    }
}
