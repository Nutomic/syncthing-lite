package net.syncthing.lite.utils

import android.webkit.MimeTypeMap

object MimeType {
    private const val DEFAULT_MIME_TYPE = "application/octet-stream"

    private fun getFromExtension(extension: String): String {
        val mimeType: String? = MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)

        return mimeType ?: DEFAULT_MIME_TYPE
    }

    fun getFromUrl(url: String) = getFromExtension(
            MimeTypeMap.getFileExtensionFromUrl(url).toLowerCase()
    )
}
