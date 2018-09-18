package net.syncthing.lite.library

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import net.syncthing.lite.BuildConfig

object DefaultLibraryManager {
    // TODO: eventually add an setting for this value
    private const val SHUTDOWN_DELAY = 1000 * 60L

    private const val LOG_TAG = "DefaultLibraryManager"
    private const val COUNTDOWN_STEP_SIZE = 1000L

    private var instance: LibraryManager? = null
    private val lock = Object()
    private val handler = Handler(Looper.getMainLooper())

    fun with(context: Context) = withApplicationContext(context.applicationContext)

    private fun withApplicationContext(context: Context): LibraryManager {
        if (instance == null) {
            synchronized(lock) {
                if (instance == null) {
                    var isRunning = false
                    var isUsed = false
                    var unusedTime = 0L
                    lateinit var countDownStep: Runnable

                    fun scheduleCountdownStep() {
                        handler.removeCallbacks(countDownStep)
                        handler.postDelayed(countDownStep, COUNTDOWN_STEP_SIZE)
                    }

                    fun cancelCountdown() {
                        unusedTime = 0
                        handler.removeCallbacks(countDownStep)
                    }

                    countDownStep = Runnable {
                        unusedTime += COUNTDOWN_STEP_SIZE

                        if (unusedTime >= SHUTDOWN_DELAY) {
                            instance!!.shutdownIfThereAreZeroUsers {
                                // ignore the result, we are informed using the isRunningListener too
                            }
                        } else {
                            // update notification
                            LibraryConnectionService.notifyRunningAndUnused((SHUTDOWN_DELAY - unusedTime) / 1000, context)

                            scheduleCountdownStep()
                        }
                    }

                    instance = LibraryManager(
                            synchronousInstanceCreator = { LibraryInstance(context) },
                            userCounterListener = {
                                newUserCounter ->

                                if (BuildConfig.DEBUG) {
                                    Log.d(LOG_TAG, "user counter updated to $newUserCounter")
                                }

                                val newIsUsed = newUserCounter > 0

                                if (newIsUsed != isUsed) {
                                    isUsed = newIsUsed

                                    if (isUsed) {
                                        cancelCountdown()

                                        LibraryConnectionService.notifyRunningAndUsed(context)
                                    } else {
                                        scheduleCountdownStep()
                                    }
                                }
                            },
                            isRunningListener = {
                                newIsRunning ->

                                if (newIsRunning != isRunning) {
                                    isRunning = newIsRunning

                                    if (!isRunning) {
                                        cancelCountdown()

                                        // hide the notification
                                        LibraryConnectionService.notifyShutDown(context)
                                    }
                                }
                            }
                    )
                }
            }
        }

        return instance!!
    }
}