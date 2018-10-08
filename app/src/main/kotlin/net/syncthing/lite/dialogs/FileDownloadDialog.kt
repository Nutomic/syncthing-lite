package net.syncthing.lite.dialogs

import android.app.ProgressDialog
import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.support.v4.content.FileProvider
import android.util.Log
import android.webkit.MimeTypeMap
import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.R
import net.syncthing.lite.library.DownloadFileTask
import org.apache.commons.io.FilenameUtils
import org.jetbrains.anko.doAsync
import org.jetbrains.anko.newTask
import org.jetbrains.anko.toast
import java.io.File

class FileDownloadDialog(private val context: Context, private val syncthingClient: SyncthingClient,
                         private val fileInfo: FileInfo) {

    private val Tag = "FileDownloadDialog"
    private lateinit var progressDialog: ProgressDialog
    private var downloadFileTask: DownloadFileTask? = null

    fun show() {
        showDialog()
        doAsync {
            downloadFileTask = DownloadFileTask(context, syncthingClient, fileInfo,
                    this@FileDownloadDialog::onProgress, this@FileDownloadDialog::onComplete,
                    this@FileDownloadDialog::onError)
        }
    }

    private fun showDialog() {
        progressDialog = ProgressDialog(context)
        progressDialog.setMessage(context.getString(R.string.dialog_downloading_file, fileInfo.fileName))
        progressDialog.setProgressStyle(ProgressDialog.STYLE_HORIZONTAL)
        progressDialog.setCancelable(true)
        progressDialog.setOnCancelListener { downloadFileTask?.cancel() }
        progressDialog.isIndeterminate = true
        progressDialog.show()
    }

    private fun onProgress(downloadFileTask: DownloadFileTask, fileDownloadObserver: BlockPuller.FileDownloadObserver) {
        progressDialog.isIndeterminate = false
        progressDialog.max = (fileInfo.size as Long).toInt()
        progressDialog.progress = (fileDownloadObserver.progress() * fileInfo.size!!).toInt()
    }

    private fun onComplete(file: File) {
        progressDialog.dismiss()
        val mimeType = MimeTypeMap.getSingleton().getMimeTypeFromExtension(FilenameUtils.getExtension(file.name))
        val intent = Intent(Intent.ACTION_VIEW)
        val uri = FileProvider.getUriForFile(this@FileDownloadDialog.context, "net.syncthing.lite.fileprovider", file)
        intent.setDataAndType(uri, mimeType)
        intent.newTask()
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        try {
            this@FileDownloadDialog.context.startActivity(intent)
        } catch (e: ActivityNotFoundException) {
            this@FileDownloadDialog.context.toast(R.string.toast_open_file_failed)
            Log.w(Tag, "No handler found for file " + file.name, e)
        }
    }

    private fun onError() {
        progressDialog.cancel()
        this@FileDownloadDialog.context.toast(R.string.toast_file_download_failed)
    }
}
