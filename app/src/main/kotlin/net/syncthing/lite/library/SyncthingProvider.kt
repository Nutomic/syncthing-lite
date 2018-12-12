package net.syncthing.lite.library

import android.database.Cursor
import android.database.MatrixCursor
import android.os.CancellationSignal
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import net.syncthing.java.bep.index.browser.DirectoryContentListing
import net.syncthing.java.bep.index.browser.DirectoryNotFoundListing
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.lite.R
import net.syncthing.lite.utils.MimeType
import java.io.FileNotFoundException

class SyncthingProvider : DocumentsProvider() {

    companion object {
        private const val Tag = "SyncthingProvider"

        private val DefaultRootProjection = arrayOf(
                Root.COLUMN_ROOT_ID,
                Root.COLUMN_FLAGS,
                Root.COLUMN_TITLE,
                Root.COLUMN_SUMMARY,
                Root.COLUMN_DOCUMENT_ID,
                Root.COLUMN_ICON
        )

        private val DefaultDocumentProjection = arrayOf(
                Document.COLUMN_DOCUMENT_ID,
                Document.COLUMN_DISPLAY_NAME,
                Document.COLUMN_SIZE,
                Document.COLUMN_MIME_TYPE,
                Document.COLUMN_LAST_MODIFIED,
                Document.COLUMN_FLAGS
        )
    }

    override fun onCreate(): Boolean {
        Log.d(Tag, "onCreate()")
        return true
    }

    // this instance is not started -> it connects and disconnects on demand
    private val libraryManager: LibraryManager by lazy { DefaultLibraryManager.with(context) }

    override fun queryRoots(projection: Array<String>?): Cursor {
        Log.d(Tag, "queryRoots($projection)")

        return runBlocking {
            libraryManager.withLibrary { instance ->
                MatrixCursor(projection ?: DefaultRootProjection).apply {
                    instance.folderBrowser.folderInfoAndStatusList().forEach { folder ->
                        newRow().apply {
                            add(Root.COLUMN_ROOT_ID, folder.info.folderId)
                            add(Root.COLUMN_SUMMARY, folder.info.label)
                            add(Root.COLUMN_FLAGS, 0)
                            add(Root.COLUMN_TITLE, context.getString(R.string.app_name))
                            add(Root.COLUMN_DOCUMENT_ID, getDocIdForFile(folder.info))
                            add(Root.COLUMN_ICON, R.mipmap.ic_launcher)
                        }
                    }
                }
            }
        }
    }

    override fun queryChildDocuments(parentDocumentId: String, projection: Array<String>?,
                                     sortOrder: String?): Cursor {
        Log.d(Tag, "queryChildDocuments($parentDocumentId, $projection, $sortOrder)")

        return runBlocking {
            libraryManager.withLibrary { instance ->
                val listing = instance.indexBrowser.getDirectoryListing(
                        folder = getFolderIdForDocId(parentDocumentId),
                        path = getPathForDocId(parentDocumentId)
                )

                when (listing) {
                    is DirectoryNotFoundListing -> throw FileNotFoundException()
                    is DirectoryContentListing -> {
                        val result = MatrixCursor(projection ?: DefaultDocumentProjection)

                        listing.entries.forEach { entry ->
                            includeFile(result, entry)
                        }

                        result
                    }
                }
            }
        }
    }

    override fun queryDocument(documentId: String, projection: Array<String>?): Cursor {
        Log.d(Tag, "queryDocument($documentId, $projection)")

        return runBlocking {
            libraryManager.withLibrary {  instance ->
                val fileInfo = instance.indexBrowser.getFileInfoByAbsolutePathAllowNull(
                        folder = getFolderIdForDocId(documentId),
                        path = getPathForDocId(documentId)
                ) ?: throw FileNotFoundException()

                MatrixCursor(projection ?: DefaultDocumentProjection).apply {
                    includeFile(this, fileInfo)
                }
            }
        }
    }

    @Throws(FileNotFoundException::class)
    override fun openDocument(documentId: String, mode: String, signal: CancellationSignal?):
            ParcelFileDescriptor {
        Log.d(Tag, "openDocument($documentId, $mode, $signal)")

        val accessMode = ParcelFileDescriptor.parseMode(mode)

        if (accessMode != ParcelFileDescriptor.MODE_READ_ONLY) {
            throw NotImplementedError()
        }

        return runBlocking {
            libraryManager.withLibrary { instance ->
                val fileInfo = instance.indexBrowser.getFileInfoByAbsolutePathAllowNull(
                        folder = getFolderIdForDocId(documentId),
                        path = getPathForDocId(documentId)
                ) ?: throw FileNotFoundException()

                signal?.setOnCancelListener {
                    this.coroutineContext.cancel()
                }

                val outputFile = DownloadFileTask.downloadFileCoroutine(
                        externalCacheDir = context.externalCacheDir,
                        syncthingClient = instance.syncthingClient,
                        fileInfo = fileInfo,
                        onProgress = { /* ignore the progress */ }
                )

                ParcelFileDescriptor.open(outputFile, ParcelFileDescriptor.MODE_READ_ONLY)
            }
        }
    }

    private fun includeFile(result: MatrixCursor, fileInfo: FileInfo) {
        result.newRow().apply {
            add(Document.COLUMN_DOCUMENT_ID, getDocIdForFile(fileInfo))
            add(Document.COLUMN_DISPLAY_NAME, fileInfo.fileName)
            add(Document.COLUMN_SIZE, fileInfo.size)
            add(
                    Document.COLUMN_MIME_TYPE,
                    if (fileInfo.isDirectory())
                        Document.MIME_TYPE_DIR
                    else
                        MimeType.getFromUrl(fileInfo.fileName)
            )
            add(Document.COLUMN_LAST_MODIFIED, fileInfo.lastModified)
            add(Document.COLUMN_FLAGS, 0)
        }
    }

    private fun getFolderIdForDocId(docId: String) = docId.split(":")[0]

    private fun getPathForDocId(docId: String) = docId.split(":")[1]

    private fun getDocIdForFile(folderInfo: FolderInfo) = folderInfo.folderId + ":"

    private fun getDocIdForFile(fileInfo: FileInfo) = fileInfo.folder + ":" + fileInfo.path
}
