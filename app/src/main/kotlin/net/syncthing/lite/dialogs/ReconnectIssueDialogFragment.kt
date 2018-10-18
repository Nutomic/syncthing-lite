package net.syncthing.lite.dialogs

import android.os.Bundle
import android.support.v4.app.DialogFragment
import android.support.v4.app.FragmentActivity
import android.support.v7.app.AlertDialog
import net.syncthing.lite.R
import org.jetbrains.anko.defaultSharedPreferences

class ReconnectIssueDialogFragment: DialogFragment() {
    override fun onCreateDialog(savedInstanceState: Bundle?) = AlertDialog.Builder(context!!, theme)
            .setMessage(R.string.dialog_warning_reconnect_problem)
            .setPositiveButton(android.R.string.ok) { _, _ ->
                context!!.defaultSharedPreferences.edit()
                        .putBoolean(SETTINGS_PARAM, true)
                        .apply()
            }
            .create()

    companion object {
        private const val DIALOG_TAG = "ReconnectIssueDialog"
        private const val SETTINGS_PARAM = "has_educated_about_reconnect_issues"

        fun showIfNeeded(activity: FragmentActivity) {
            if (!activity.defaultSharedPreferences.getBoolean(SETTINGS_PARAM, false)) {
                if (activity.supportFragmentManager.findFragmentByTag(DIALOG_TAG) == null) {
                    ReconnectIssueDialogFragment().show(activity.supportFragmentManager, DIALOG_TAG)
                }
            }
        }
    }
}
