package net.syncthing.lite.library

import android.content.ContentProvider
import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.provider.OpenableColumns
import java.io.File
import java.io.IOException

class CacheFileProvider: ContentProvider() {
    companion object {
        const val AUTHORITY = "net.syncthing.lite.fileprovider"
    }

    override fun onCreate() = true

    override fun insert(uri: Uri?, values: ContentValues?): Uri {
        throw NotImplementedError()
    }

    override fun update(uri: Uri?, values: ContentValues?, selection: String?, selectionArgs: Array<out String>?): Int {
        throw NotImplementedError()
    }

    override fun delete(uri: Uri?, selection: String?, selectionArgs: Array<out String>?): Int {
        throw NotImplementedError()
    }

    override fun query(uri: Uri, projection: Array<out String>?, selection: String?, selectionArgs: Array<out String>?, sortOrder: String?): Cursor {
        val url = CacheFileProviderUrl.fromUri(uri)
        val file = url.getFile(context)

        val resultProjection = projection ?: arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE)
        val resultCursor = MatrixCursor(resultProjection)

        if (file.exists()) {
            val builder = resultCursor.newRow()

            for (row in resultProjection) {
                when (row) {
                    OpenableColumns.DISPLAY_NAME -> builder.add(url.filename)
                    OpenableColumns.SIZE -> builder.add(file.length())
                    else -> builder.add(null)
                }
            }
        }

        return resultCursor
    }

    override fun getType(uri: Uri): String = CacheFileProviderUrl.fromUri(uri).mimeType

    override fun openFile(uri: Uri, mode: String): ParcelFileDescriptor {
        if (mode == "r") {
            val url = CacheFileProviderUrl.fromUri(uri)
            val file = url.getFile(context)

            return ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
        } else {
            throw IOException("illegal mode")
        }
    }
}

data class CacheFileProviderUrl(
        val pathInCacheDirectory: String,
        val filename: String,
        val mimeType: String
) {
    companion object {
        private const val PATH = "path"
        private const val FILENAME = "filename"
        private const val MIME_TYPE = "mimeType"

        fun fromUri(uri: Uri) = CacheFileProviderUrl(
                pathInCacheDirectory = uri.getQueryParameter(PATH),
                filename = uri.getQueryParameter(FILENAME),
                mimeType = uri.getQueryParameter(MIME_TYPE)
        )

        fun fromFile(file: File, filename: String, mimeType: String, context: Context) = CacheFileProviderUrl(
                filename = filename,
                mimeType = mimeType,
                pathInCacheDirectory = file.toRelativeString(context.externalCacheDir)
        )
    }

    val serialized: Uri by lazy {
        Uri.Builder()
                .scheme("content")
                .authority(CacheFileProvider.AUTHORITY)
                .appendQueryParameter(PATH, pathInCacheDirectory)
                .appendQueryParameter(FILENAME, filename)
                .appendQueryParameter(MIME_TYPE, mimeType)
                .build()
    }

    fun getFile(context: Context): File {
        return File(context.externalCacheDir, pathInCacheDirectory)
    }
}
