package net.syncthing.lite.utils

import android.content.Context
import android.net.Uri
import android.os.Build
import android.provider.OpenableColumns
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.lite.R
import net.syncthing.lite.library.LibraryManager
import org.apache.commons.lang3.StringUtils.capitalize
import org.jetbrains.anko.toast
import java.io.IOException
import java.security.InvalidParameterException
import java.util.*

object Util {

    fun getDeviceName(): String {
        val manufacturer = Build.MANUFACTURER ?: ""
        val model = Build.MODEL ?: ""
        val deviceName =
                if (model.startsWith(manufacturer)) {
                    capitalize(model)
                } else {
                    capitalize(manufacturer) + " " + model
                }
        return deviceName ?: "android"
    }

    fun getContentFileName(context: Context, uri: Uri): String {
        context.contentResolver.query(uri, null, null, null, null, null).use { cursor ->
            if (cursor == null || !cursor.moveToFirst()) {
                throw InvalidParameterException("Cursor is null or empty")
            }
            return cursor.getString(cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME))
        }
    }

    @Throws(IOException::class)
    fun importDeviceId(libraryManager: LibraryManager, context: Context, deviceId: String, onComplete: () -> Unit) {
        val newDeviceId = DeviceId(deviceId.toUpperCase(Locale.US))

        GlobalScope.launch (Dispatchers.Main) {
            libraryManager.withLibrary { library ->
                val didAddDevice = library.configuration.update { oldConfig ->
                    if (oldConfig.peers.find { it.deviceId == newDeviceId } != null) {
                        // already known

                        oldConfig
                    } else {
                        oldConfig.copy(
                                peers = oldConfig.peers + DeviceInfo(newDeviceId, newDeviceId.shortId)
                        )
                    }
                }

                if (didAddDevice) {
                    library.configuration.persistLater()
                    library.syncthingClient.connectToNewlyAddedDevices()

                    context.toast(context.getString(R.string.device_import_success, newDeviceId.shortId))
                    onComplete()
                } else {
                    context.toast(context.getString(R.string.device_already_known, newDeviceId.shortId))
                }

                null
            }
        }
    }
}
