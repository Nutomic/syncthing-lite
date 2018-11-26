package net.syncthing.lite.android

import android.app.Application
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.util.Log
import net.syncthing.lite.BuildConfig
import org.jetbrains.anko.defaultSharedPreferences
import java.io.PrintWriter
import java.io.StringWriter

class Application: Application() {
    companion object {
        private const val LOG_TAG = "Application"
        private const val PREF_ENABLE_CRASH_HANDLER = "crash_handler"
    }

    override fun onCreate() {
        super.onCreate()

        val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
        val defaultHandler = Thread.getDefaultUncaughtExceptionHandler()

        if (defaultHandler == null) {
            Log.w(LOG_TAG, "could not get default crash handler")
        }

        Thread.setDefaultUncaughtExceptionHandler { thread, ex ->
            Log.w(LOG_TAG, "app crashed", ex)

            val enableCustomCrashHandling = defaultSharedPreferences.getBoolean(PREF_ENABLE_CRASH_HANDLER, false)

            if (enableCustomCrashHandling) {
                clipboard.primaryClip = ClipData.newPlainText(
                        "stacktrace",
                        StringWriter().apply {
                            append("Version: ").append(BuildConfig.VERSION_NAME).append('\n')
                            append(Log.getStackTraceString(ex)).append('\n')
                            ex.printStackTrace(PrintWriter(this))
                        }.buffer.toString()
                )
            }

            if (defaultHandler != null) {
                defaultHandler.uncaughtException(thread, ex)
            } else {
                System.exit(1)
            }
        }
    }
}
