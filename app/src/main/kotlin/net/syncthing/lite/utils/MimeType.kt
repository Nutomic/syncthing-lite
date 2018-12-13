package net.syncthing.lite.utils

import android.webkit.MimeTypeMap
import net.syncthing.java.core.utils.PathUtils

object MimeType {
    private const val DEFAULT_MIME_TYPE = "application/octet-stream"

    private fun getFromExtension(extension: String): String {
        val mimeType: String? = MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)

        return mimeType ?: DEFAULT_MIME_TYPE
    }

    fun getFromFilename(path: String) = getFromExtension(
            PathUtils.getFileExtensionFromFilename(path).toLowerCase()
    )
}
