package net.syncthing.lite.library

import android.database.Cursor
import android.database.MatrixCursor
import android.os.CancellationSignal
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.lite.R
import java.io.File
import java.io.FileNotFoundException
import java.net.URLConnection
import java.util.concurrent.CountDownLatch

class SyncthingProvider : DocumentsProvider() {

    companion object {
        private const val Tag = "SyncthingProvider"
        private val DefaultRootProjection = arrayOf(
                Root.COLUMN_ROOT_ID,
                Root.COLUMN_FLAGS,
                Root.COLUMN_TITLE,
                Root.COLUMN_SUMMARY,
                Root.COLUMN_DOCUMENT_ID,
                Root.COLUMN_ICON)
        private val DefaultDocumentProjection = arrayOf(
                Document.COLUMN_DOCUMENT_ID,
                Document.COLUMN_DISPLAY_NAME,
                Document.COLUMN_SIZE,
                Document.COLUMN_MIME_TYPE,
                Document.COLUMN_LAST_MODIFIED,
                Document.COLUMN_FLAGS)
    }

    override fun onCreate(): Boolean {
        Log.d(Tag, "onCreate()")
        return true
    }

    // this instance is not started -> it connects and disconnects on demand
    private val libraryHandler: LibraryHandler by lazy { LibraryHandler(context) }

    override fun queryRoots(projection: Array<String>?): Cursor {
        Log.d(Tag, "queryRoots($projection)")
        val latch = CountDownLatch(1)
        var folders: List<Pair<FolderInfo, FolderStats>>? = null
        libraryHandler.folderBrowser { folderBrowser ->
            folders = folderBrowser.folderInfoAndStatsList()
            latch.countDown()
        }
        latch.await()

        val result = MatrixCursor(projection ?: DefaultRootProjection)
        folders!!.forEach { folder ->
            val row = result.newRow()
            row.add(Root.COLUMN_ROOT_ID, folder.first.folderId)
            row.add(Root.COLUMN_SUMMARY, folder.first.label)
            row.add(Root.COLUMN_FLAGS, 0)
            row.add(Root.COLUMN_TITLE, context.getString(R.string.app_name))
            row.add(Root.COLUMN_DOCUMENT_ID, getDocIdForFile(folder.first))
            row.add(Root.COLUMN_ICON, R.mipmap.ic_launcher)
        }
        return result
    }

    override fun queryChildDocuments(parentDocumentId: String, projection: Array<String>?,
                                     sortOrder: String?): Cursor {
        Log.d(Tag, "queryChildDocuments($parentDocumentId, $projection, $sortOrder)")
        val result = MatrixCursor(projection ?: DefaultDocumentProjection)
        getIndexBrowser(getFolderIdForDocId(parentDocumentId))
                .listFiles(getPathForDocId(parentDocumentId))
                .forEach { fileInfo ->
                    includeFile(result, fileInfo)
                }
        return result
    }

    override fun queryDocument(documentId: String, projection: Array<String>?): Cursor {
        Log.d(Tag, "queryDocument($documentId, $projection)")
        val result = MatrixCursor(projection ?: DefaultDocumentProjection)
        val fileInfo = getIndexBrowser(getFolderIdForDocId(documentId))
                .getFileInfoByAbsolutePath(getPathForDocId(documentId))
        includeFile(result, fileInfo)
        return result
    }

    @Throws(FileNotFoundException::class)
    override fun openDocument(documentId: String, mode: String, signal: CancellationSignal?):
            ParcelFileDescriptor {
        Log.d(Tag, "openDocument($documentId, $mode, $signal)")
        val fileInfo = FileInfo(folder = getFolderIdForDocId(documentId),
                path = getPathForDocId(documentId), type = FileInfo.FileType.FILE)
        val accessMode = ParcelFileDescriptor.parseMode(mode)
        if (accessMode != ParcelFileDescriptor.MODE_READ_ONLY) {
            throw NotImplementedError()
        }

        val latch = CountDownLatch(1)
        var outputFile: File? = null
        libraryHandler.syncthingClient { syncthingClient ->
            DownloadFileTask(context, syncthingClient, fileInfo,
                    { t, _ -> if (signal?.isCanceled == true) t.cancel() }, {
                outputFile = it
                latch.countDown()
            }, {})
        }
        latch.await()
        return ParcelFileDescriptor.open(outputFile, ParcelFileDescriptor.MODE_READ_ONLY)
    }

    private fun includeFile(result: MatrixCursor, fileInfo: FileInfo) {
        val row = result.newRow()
        row.add(Document.COLUMN_DOCUMENT_ID, getDocIdForFile(fileInfo))
        row.add(Document.COLUMN_DISPLAY_NAME, fileInfo.fileName)
        row.add(Document.COLUMN_SIZE, fileInfo.size)
        val mime = if (fileInfo.isDirectory()) Document.MIME_TYPE_DIR
                   else URLConnection.guessContentTypeFromName(fileInfo.fileName)
        row.add(Document.COLUMN_MIME_TYPE, mime)
        row.add(Document.COLUMN_LAST_MODIFIED, fileInfo.lastModified)
        row.add(Document.COLUMN_FLAGS, 0)
    }

    private fun getFolderIdForDocId(docId: String) = docId.split(":")[0]

    private fun getPathForDocId(docId: String) = docId.split(":")[1]

    private fun getDocIdForFile(folderInfo: FolderInfo) = folderInfo.folderId + ":"

    private fun getDocIdForFile(fileInfo: FileInfo) = fileInfo.folder + ":" + fileInfo.path

    private fun getIndexBrowser(folderId: String): IndexBrowser {
        val latch = CountDownLatch(1)
        var indexBrowser: IndexBrowser? = null
        libraryHandler.syncthingClient {
            indexBrowser = it.indexHandler.newIndexBrowser(folderId)
            latch.countDown()
        }
        latch.await()
        return indexBrowser!!
    }
}
