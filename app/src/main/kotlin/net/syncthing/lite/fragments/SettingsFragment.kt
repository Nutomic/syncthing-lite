package net.syncthing.lite.fragments

import android.os.Bundle
import android.support.v7.preference.EditTextPreference
import android.support.v7.preference.PreferenceFragmentCompat
import net.syncthing.lite.R
import net.syncthing.lite.activities.SyncthingActivity

class SettingsFragment : PreferenceFragmentCompat() {

    override fun onCreatePreferences(savedInstanceState: Bundle?, rootKey: String?) {
        addPreferencesFromResource(R.xml.preferences)

        val localDeviceName = findPreference("local_device_name") as EditTextPreference
        val appVersion      = findPreference("app_version")
        val forceStop       = findPreference("force_stop")

        (activity as SyncthingActivity?)?.let { activity ->
            val versionName = activity.packageManager.getPackageInfo(activity.packageName, 0)?.versionName
            appVersion.summary = versionName

            activity.libraryHandler.configuration { localDeviceName.text = it.localDeviceName }
            localDeviceName.setOnPreferenceChangeListener { _, _ ->
                activity.libraryHandler.configuration { conf ->
                    conf.localDeviceName = localDeviceName.text
                    conf.persistLater()
                }
                true
            }
        }

        forceStop.setOnPreferenceClickListener {
            System.exit(0)

            true
        }
    }
}
