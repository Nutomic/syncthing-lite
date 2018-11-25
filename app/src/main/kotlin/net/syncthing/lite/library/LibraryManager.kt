package net.syncthing.lite.library

import android.os.Handler
import android.os.Looper
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

/**
 * This class manages the access to an LibraryInstance
 *
 * Users can get an instance with startLibraryUsage()
 * If they are done with it, the should call stopLibraryUsage()
 * After this, it's NOT safe to continue using the received LibraryInstance
 *
 * Every call to startLibraryUsage should be followed by an call to stopLibraryUsage,
 * even if the callback was not called yet. It can still be called, so users should watch out.
 *
 * All listeners are executed at the UI Thread (except the synchronousInstanceCreator)
 *
 * The userCounterListener is always called before the isRunningListener
 *
 * The listeners are called for all changes, nothing is skipped or batched
 */
class LibraryManager (
        val synchronousInstanceCreator: () -> LibraryInstance,
        val userCounterListener: (Int) -> Unit = {},
        val isRunningListener: (isRunning: Boolean) -> Unit = {}
) {
    companion object {
        private val handler = Handler(Looper.getMainLooper())
    }

    // this must be a SingleThreadExecutor to avoid race conditions
    // only this Thread should access instance and userCounter
    private val startStopExecutor = Executors.newSingleThreadExecutor()

    private var instance: LibraryInstance? = null
    private var userCounter = 0

    fun startLibraryUsage(callback: (LibraryInstance) -> Unit) {
        startStopExecutor.submit {
            val newUserCounter = ++userCounter
            handler.post { userCounterListener(newUserCounter) }

            if (instance == null) {
                instance = synchronousInstanceCreator()
                handler.post { isRunningListener(true) }
            }

            handler.post { callback(instance!!) }
        }
    }

    suspend fun startLibraryUsageCoroutine(): LibraryInstance {
        return suspendCoroutine { continuation ->
            startLibraryUsage { instance ->
                continuation.resume(instance)
            }
        }
    }

    fun stopLibraryUsage() {
        startStopExecutor.submit {
            val newUserCounter = --userCounter

            if (newUserCounter < 0) {
                userCounter = 0

                throw IllegalStateException("can not stop library usage if there are 0 users")
            }

            handler.post { userCounterListener(newUserCounter) }
        }
    }

    fun shutdownIfThereAreZeroUsers(listener: (wasShutdownPerformed: Boolean) -> Unit = {}) {
        startStopExecutor.submit {
            if (userCounter == 0) {
                instance?.shutdown()
                instance = null

                handler.post { isRunningListener(false) }
                handler.post { listener(true) }
            } else {
                handler.post { listener(false) }
            }
        }
    }
}
