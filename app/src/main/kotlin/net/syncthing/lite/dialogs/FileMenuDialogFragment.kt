package net.syncthing.lite.dialogs

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.support.design.widget.BottomSheetDialogFragment
import android.support.v4.app.FragmentManager
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.databinding.DialogFileBinding
import net.syncthing.lite.dialogs.downloadfile.DownloadFileDialogFragment
import net.syncthing.lite.dialogs.downloadfile.DownloadFileSpec
import net.syncthing.lite.utils.MimeType

class FileMenuDialogFragment: BottomSheetDialogFragment() {
    companion object {
        private const val ARG_FILE_SPEC = "file spec"
        private const val TAG = "DownloadFileDialog"
        private const val REQ_SAVE_AS = 1

        fun newInstance(fileInfo: FileInfo) = newInstance(DownloadFileSpec(
                folder = fileInfo.folder,
                path = fileInfo.path,
                fileName = fileInfo.fileName
        ))

        fun newInstance(fileSpec: DownloadFileSpec) = FileMenuDialogFragment().apply {
            arguments = Bundle().apply {
                putSerializable(ARG_FILE_SPEC, fileSpec)
            }
        }
    }

    val fileSpec: DownloadFileSpec by lazy {
        arguments!!.getSerializable(ARG_FILE_SPEC) as DownloadFileSpec
    }

    override fun onCreateView(inflater: LayoutInflater, container: ViewGroup?, savedInstanceState: Bundle?): View? {
        val binding = DialogFileBinding.inflate(inflater, container, false)

        binding.filename = fileSpec.fileName

        binding.saveAsButton.setOnClickListener {
            startActivityForResult(
                    Intent(Intent.ACTION_CREATE_DOCUMENT).apply {
                        addCategory(Intent.CATEGORY_OPENABLE)

                        type = MimeType.getFromFilename(fileSpec.fileName)

                        putExtra(Intent.EXTRA_TITLE, fileSpec.fileName)
                    },
                    REQ_SAVE_AS
            )
        }

        return binding.root
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        when (requestCode) {
            REQ_SAVE_AS -> {
                if (resultCode == Activity.RESULT_OK) {
                    DownloadFileDialogFragment.newInstance(fileSpec, data!!.data!!).show(fragmentManager)
                    dismiss()
                }
            }
            else -> super.onActivityResult(requestCode, resultCode, data)
        }
    }

    fun show(fragmentManager: FragmentManager?) {
        show(fragmentManager, TAG)
    }
}
