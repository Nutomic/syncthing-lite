package net.syncthing.lite.error

import android.content.Context
import org.jetbrains.anko.defaultSharedPreferences

object ErrorStorage {
    private const val PREF_KEY = "last_error"

    fun reportError(context: Context, error: String) {
        // this uses commit because the App could be quit directly after that
        context.defaultSharedPreferences.edit()
                .putString(PREF_KEY, error)
                .commit()
    }

    fun getLastErrorReport(context: Context) = context.defaultSharedPreferences.getString(PREF_KEY, "there is no saved report")
}
