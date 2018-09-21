package net.syncthing.lite.library

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import net.syncthing.lite.BuildConfig
import net.syncthing.lite.R
import org.jetbrains.anko.defaultSharedPreferences

object DefaultLibraryManager {
    private const val LOG_TAG = "DefaultLibraryManager"

    private var instance: LibraryManager? = null
    private val lock = Object()
    private val handler = Handler(Looper.getMainLooper())

    fun with(context: Context) = withApplicationContext(context.applicationContext)

    private fun withApplicationContext(context: Context): LibraryManager {
        if (instance == null) {
            synchronized(lock) {
                if (instance == null) {
                    val shutdownRunnable = Runnable {
                        instance!!.shutdownIfThereAreZeroUsers()
                    }

                    fun scheduleShutdown() {
                        val shutdownDelay = context.defaultSharedPreferences.getString(
                                "shutdown_delay",
                                context.getString(R.string.default_shutdown_delay)
                        ).toLong()

                        handler.postDelayed(shutdownRunnable, shutdownDelay)
                    }

                    fun cancelShutdown() {
                        handler.removeCallbacks(shutdownRunnable)
                    }

                    instance = LibraryManager(
                            synchronousInstanceCreator = { LibraryInstance(context) },
                            userCounterListener = {
                                newUserCounter ->

                                if (BuildConfig.DEBUG) {
                                    Log.d(LOG_TAG, "user counter updated to $newUserCounter")
                                }

                                val isUsed = newUserCounter > 0

                                if (isUsed) {
                                    cancelShutdown()
                                } else {
                                    scheduleShutdown()
                                }
                            }
                    )
                }
            }
        }

        return instance!!
    }
}
