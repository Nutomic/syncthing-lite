package net.syncthing.lite.dialogs.downloadfile

import android.app.Dialog
import android.app.ProgressDialog
import android.arch.lifecycle.Observer
import android.arch.lifecycle.ViewModelProviders
import android.content.ActivityNotFoundException
import android.content.DialogInterface
import android.content.Intent
import android.os.Bundle
import android.support.v4.app.DialogFragment
import android.support.v4.app.FragmentManager
import android.util.Log
import android.webkit.MimeTypeMap
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.BuildConfig
import net.syncthing.lite.R
import net.syncthing.lite.library.CacheFileProviderUrl
import net.syncthing.lite.library.LibraryHandler
import org.apache.commons.io.FilenameUtils
import org.jetbrains.anko.newTask
import org.jetbrains.anko.toast

class DownloadFileDialogFragment : DialogFragment() {
    companion object {
        private const val ARG_FILE_SPEC = "file spec"
        private const val TAG = "DownloadFileDialog"

        fun newInstance(fileInfo: FileInfo) = newInstance(DownloadFileSpec(
                folder = fileInfo.folder,
                path = fileInfo.path,
                fileName = fileInfo.fileName
        ))

        fun newInstance(fileSpec: DownloadFileSpec) = DownloadFileDialogFragment().apply {
            arguments = Bundle().apply {
                putSerializable(ARG_FILE_SPEC, fileSpec)
            }
        }
    }

    val model: DownloadFileDialogViewModel by lazy {
        ViewModelProviders.of(this).get(DownloadFileDialogViewModel::class.java)
    }

    override fun onCreateDialog(savedInstanceState: Bundle?): Dialog {
        val fileSpec = arguments!!.getSerializable(ARG_FILE_SPEC) as DownloadFileSpec

        model.init(
                libraryHandler = LibraryHandler(context!!),
                fileSpec = fileSpec,
                externalCacheDir = context!!.externalCacheDir
        )

        val progressDialog = ProgressDialog(context).apply {
            setMessage(context!!.getString(R.string.dialog_downloading_file, fileSpec.fileName))
            setProgressStyle(ProgressDialog.STYLE_HORIZONTAL)
            isCancelable = true
            isIndeterminate = true
            max = DownloadFileStatusRunning.MAX_PROGRESS
        }

        model.status.observe(this, Observer {
            status ->

            when (status) {
                is DownloadFileStatusRunning -> {
                    progressDialog.apply {
                        isIndeterminate = false
                        progress = status.progress
                    }
                }
                is DownloadFileStatusDone -> {
                    dismissAllowingStateLoss()

                    val mimeType = MimeTypeMap.getSingleton().getMimeTypeFromExtension(FilenameUtils.getExtension(fileSpec.fileName))

                    try {
                        context!!.startActivity(
                                Intent(Intent.ACTION_VIEW)
                                        .setDataAndType(
                                                CacheFileProviderUrl.fromFile(
                                                        filename = fileSpec.fileName,
                                                        mimeType = mimeType,
                                                        file = status.file,
                                                        context = context!!
                                                ).serialized,
                                                mimeType
                                        )
                                        .newTask()
                                        .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                        )
                    } catch (e: ActivityNotFoundException) {
                        if (BuildConfig.DEBUG) {
                            Log.w(TAG, "No handler found for file " + status.file.name, e)
                        }

                        context!!.toast(R.string.toast_open_file_failed)
                    }
                }
                is DownloadFileStatusFailed -> {
                    dismissAllowingStateLoss()

                    context!!.toast(R.string.toast_file_download_failed)
                }
            }
        })

        return progressDialog
    }

    override fun onCancel(dialog: DialogInterface?) {
        super.onCancel(dialog)

        model.cancel()
    }

    fun show(fragmentManager: FragmentManager?) {
        show(fragmentManager, TAG)
    }
}
