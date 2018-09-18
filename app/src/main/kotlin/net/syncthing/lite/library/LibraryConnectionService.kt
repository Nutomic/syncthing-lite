package net.syncthing.lite.library

import android.app.Notification
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import android.support.v4.app.NotificationCompat
import android.support.v4.content.ContextCompat
import net.syncthing.lite.R

/**
 * This Service does NOT do an connection. It just shows the connection notification.
 * However, by showing notifications from an Service, we tell Android that there is
 * a good reason to not kill the app process.
 */
// TODO: better notification icon (currently an arrow)
class LibraryConnectionService: Service() {
    companion object {
        private const val NOTIFICATION_ID = 1

        private const val ACTION_NOTIFY_RUNNING_AND_USED = "notify running and used"
        private const val ACTION_NOTIFY_RUNNING_AND_UNUSED = "notify running and unused"
        private const val ACTION_NOTIFY_SHUT_DOWN = "notify shut down"
        private const val EXTRA_COUNTDOWN_SECONDS = "countdown seconds"

        fun notifyRunningAndUsed(context: Context) {
            ContextCompat.startForegroundService(
                    context,
                    Intent(context, LibraryConnectionService::class.java)
                            .setAction(ACTION_NOTIFY_RUNNING_AND_USED)
            )
        }

        fun notifyRunningAndUnused(countdownSeconds: Long, context: Context) {
            ContextCompat.startForegroundService(
                    context,
                    Intent(context, LibraryConnectionService::class.java)
                            .setAction(ACTION_NOTIFY_RUNNING_AND_UNUSED)
                            .putExtra(EXTRA_COUNTDOWN_SECONDS, countdownSeconds)
            )
        }

        fun notifyShutDown(context: Context) {
            ContextCompat.startForegroundService(
                    context,
                    Intent(context, LibraryConnectionService::class.java)
                            .setAction(ACTION_NOTIFY_SHUT_DOWN)
            )
        }
    }

    val notificationManager: NotificationManager by lazy { getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager }
    var isShowingNotification = false

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        super.onStartCommand(intent, flags, startId)

        if (intent != null) {
            when (intent.action) {
                ACTION_NOTIFY_RUNNING_AND_USED -> notifyRunningAndUsed()
                ACTION_NOTIFY_RUNNING_AND_UNUSED -> notifyRunningAndUnused(intent.getLongExtra(EXTRA_COUNTDOWN_SECONDS, 0L))
                ACTION_NOTIFY_SHUT_DOWN -> notifyShutDown()
                else -> throw IllegalArgumentException()
            }
        }

        return START_NOT_STICKY
    }

    fun notifyRunningAndUsed() {
        showNotification(
                NOTIFICATION_ID,
                NotificationCompat.Builder(this)
                        .setSmallIcon(R.drawable.ic_navigate_next_white)
                        .setContentTitle(getString(R.string.app_name))
                        .setContentText("running and used")
                        .build()
        )
    }

    fun notifyRunningAndUnused(countdownSeconds: Long) {
        showNotification(
                NOTIFICATION_ID,
                NotificationCompat.Builder(this)
                        .setSmallIcon(R.drawable.ic_navigate_next_white)
                        .setContentTitle(getString(R.string.app_name))
                        .setContentText("unused - will shutdown in $countdownSeconds seconds")
                        .build()
        )
    }

    fun notifyShutDown() {
        stopForeground(true)
        stopSelf()
    }

    fun showNotification(notificationId: Int, notification: Notification) {
        if (isShowingNotification) {
            notificationManager.notify(notificationId, notification)
        } else {
            startForeground(notificationId, notification)
            isShowingNotification = true
        }
    }

    override fun onDestroy() {
        super.onDestroy()

        stopForeground(true)
    }

    override fun onBind(intent: Intent?): IBinder {
        throw NotImplementedError()
    }
}