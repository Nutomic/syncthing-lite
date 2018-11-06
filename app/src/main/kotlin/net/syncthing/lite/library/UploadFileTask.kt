package net.syncthing.lite.library

import android.content.Context
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.util.Log
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import net.syncthing.java.bep.BlockPusher
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.utils.PathUtils
import net.syncthing.lite.utils.Util
import org.apache.commons.io.IOUtils

// TODO: this should be an IntentService with notification
class UploadFileTask(context: Context, syncthingClient: SyncthingClient,
                     localFile: Uri, private val syncthingFolder: String,
                     syncthingSubFolder: String,
                     private val onProgress: (BlockPusher.FileUploadObserver) -> Unit,
                     private val onComplete: () -> Unit,
                     private val onError: () -> Unit) {

    companion object {
        private const val TAG = "UploadFileTask"
        private val handler = Handler(Looper.getMainLooper())
    }

    private val syncthingPath = PathUtils.buildPath(syncthingSubFolder, Util.getContentFileName(context, localFile))
    private val uploadStream = context.contentResolver.openInputStream(localFile)

    private var isCancelled = false

    init {
        Log.i(TAG, "Uploading file $localFile to folder $syncthingFolder:$syncthingPath")

        GlobalScope.launch {
            try {
                val blockPusher = syncthingClient.getBlockPusher(folderId = syncthingFolder)
                val observer = blockPusher.pushFile(uploadStream, syncthingFolder, syncthingPath)

                handler.post { onProgress(observer) }

                while (!observer.isCompleted()) {
                    if (isCancelled)
                        return@launch

                    observer.waitForProgressUpdate()
                    Log.i(TAG, "upload progress = ${observer.progressPercentage()}%")
                    handler.post { onProgress(observer) }
                }
                IOUtils.closeQuietly(uploadStream)
                handler.post { onComplete() }
            } catch (ex: Exception) {
                handler.post { onError() }
            }
        }
    }

    fun cancel() {
        isCancelled = true
    }
}
