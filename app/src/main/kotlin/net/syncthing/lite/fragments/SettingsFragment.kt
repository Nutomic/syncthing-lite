package net.syncthing.lite.fragments

import android.os.Bundle
import android.support.v7.preference.EditTextPreference
import android.support.v7.preference.PreferenceFragmentCompat
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import net.syncthing.lite.R
import net.syncthing.lite.library.DefaultLibraryManager

class SettingsFragment : PreferenceFragmentCompat() {

    override fun onCreatePreferences(savedInstanceState: Bundle?, rootKey: String?) {
        addPreferencesFromResource(R.xml.preferences)

        val localDeviceName = findPreference("local_device_name") as EditTextPreference
        val appVersion      = findPreference("app_version")
        val forceStop       = findPreference("force_stop")
        val libraryManager  = DefaultLibraryManager.with(context!!)

        GlobalScope.launch (Dispatchers.Main) {
            libraryManager.withLibrary { library ->
                localDeviceName.text = library.configuration.localDeviceName
            }
        }

        appVersion.summary = context!!.packageManager.getPackageInfo(context!!.packageName, 0)?.versionName

        localDeviceName.setOnPreferenceChangeListener { _, _ ->
            val newDeviceName = localDeviceName.text

            GlobalScope.launch {
                libraryManager.withLibrary { library ->
                    library.configuration.update { it.copy(localDeviceName = newDeviceName) }
                    library.configuration.persistLater()
                }
            }

            true
        }

        forceStop.setOnPreferenceClickListener {
            System.exit(0)

            true
        }
    }
}
