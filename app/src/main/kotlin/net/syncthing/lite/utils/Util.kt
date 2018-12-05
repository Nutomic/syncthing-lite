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
import net.syncthing.lite.library.LibraryHandler
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
    fun importDeviceId(libraryHandler: LibraryHandler?, context: Context?, deviceId: String,
                       onComplete: () -> Unit) {
        val deviceId2 = DeviceId(deviceId.toUpperCase(Locale.US))
        libraryHandler?.library { configuration, syncthingClient, _ ->
            if (!configuration.peerIds.contains(deviceId2)) {
                configuration.peers = configuration.peers + DeviceInfo(deviceId2, deviceId2.shortId)
                configuration.persistLater()
                syncthingClient.connectToNewlyAddedDevices()
                GlobalScope.launch (Dispatchers.Main) {
                    context?.toast(context.getString(R.string.device_import_success, deviceId2.shortId))
                    onComplete()
                }
            } else {
                GlobalScope.launch (Dispatchers.Main) {
                    context?.toast(context.getString(R.string.device_already_known, deviceId2.shortId))
                }
            }
        }
    }
}
