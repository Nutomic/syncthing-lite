package net.syncthing.lite.dialogs

import android.app.Dialog
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Bundle
import android.support.v4.app.DialogFragment
import android.support.v4.app.FragmentManager
import android.support.v7.app.AlertDialog
import android.widget.Toast
import net.syncthing.lite.R

class ErrorReportDialog: DialogFragment() {
    companion object {
        private const val REPORT = "report"
        private const val TAG = "ErrorReportDialog"

        fun newInstance(report: String) = ErrorReportDialog().apply {
            arguments = Bundle().apply {
                putString(REPORT, report)
            }
        }
    }

    override fun onCreateDialog(savedInstanceState: Bundle?): Dialog {
        val report = arguments!!.getString(REPORT)
        val clipboard = context!!.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager

        return AlertDialog.Builder(context!!)
                .setTitle(R.string.settings_last_error_title)
                .setMessage(report)
                .setPositiveButton(android.R.string.ok, null)
                .setNeutralButton(R.string.copy_to_clipboard, null)
                .create()
                .apply {
                    setOnShowListener {
                        getButton(AlertDialog.BUTTON_NEUTRAL).setOnClickListener {
                            clipboard.primaryClip = ClipData.newPlainText(
                                    context!!.getString(R.string.settings_last_error_title),
                                    report
                            )

                            Toast.makeText(context, context!!.getString(R.string.copied_to_clipboard), Toast.LENGTH_SHORT)
                                    .show()
                        }
                    }
                }
    }

    fun show(fragmentManager: FragmentManager) = show(fragmentManager, TAG)
}
