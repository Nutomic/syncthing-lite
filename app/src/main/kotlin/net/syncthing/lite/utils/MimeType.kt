package net.syncthing.lite.utils

import android.content.Context
import android.webkit.MimeTypeMap
import org.jetbrains.anko.defaultSharedPreferences
import java.util.*

object MimeType {
    private const val DEFAULT_MIME_TYPE = "application/octet-stream"

    private fun getFromExtension(extension: String): String {
        val mimeType: String? = MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)

        return mimeType ?: DEFAULT_MIME_TYPE
    }

    fun getFromUrl(url: String, convertExtensionToLowerCase: Boolean): String {
        val extension = MimeTypeMap.getFileExtensionFromUrl(url)

        return getFromExtension(
                if (convertExtensionToLowerCase)
                    extension.toLowerCase(Locale.US)
                else
                    extension
        )
    }

    fun getFromUrl(url: String, context: Context) = getFromUrl(
            url = url,
            convertExtensionToLowerCase = context.defaultSharedPreferences.getBoolean("convert_file_extension_to_lower_case_for_mime_type", false)
    )
}
