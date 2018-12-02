package net.syncthing.lite.activities

import android.app.Activity
import android.content.Intent
import android.databinding.DataBindingUtil
import android.os.Bundle
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import net.syncthing.java.bep.index.browser.DirectoryContentListing
import net.syncthing.java.bep.index.browser.DirectoryListing
import net.syncthing.java.bep.index.browser.DirectoryNotFoundListing
import net.syncthing.java.bep.index.browser.IndexBrowser
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.utils.PathUtils
import net.syncthing.lite.R
import net.syncthing.lite.adapters.FolderContentsAdapter
import net.syncthing.lite.adapters.FolderContentsListener
import net.syncthing.lite.databinding.ActivityFolderBrowserBinding
import net.syncthing.lite.dialogs.FileMenuDialogFragment
import net.syncthing.lite.dialogs.FileUploadDialog
import net.syncthing.lite.dialogs.ReconnectIssueDialogFragment
import net.syncthing.lite.dialogs.downloadfile.DownloadFileDialogFragment

class FolderBrowserActivity : SyncthingActivity() {

    companion object {
        private const val REQUEST_SELECT_UPLOAD_FILE = 171
        private const val STATUS_PATH = "path"
        const val EXTRA_FOLDER_NAME = "folder_name"
    }

    private lateinit var folder: String

    private val path = ConflatedBroadcastChannel<String>()
    private val listing = ConflatedBroadcastChannel<DirectoryListing?>(null)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val binding: ActivityFolderBrowserBinding = DataBindingUtil.setContentView(this, R.layout.activity_folder_browser)
        val adapter = FolderContentsAdapter()

        binding.listView.adapter = adapter
        binding.mainListViewUploadHereButton.setOnClickListener {
            startActivityForResult(
                    Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
                        addCategory(Intent.CATEGORY_OPENABLE)
                        type = "*/*"
                    },
                    REQUEST_SELECT_UPLOAD_FILE
            )
        }
        adapter.listener = object: FolderContentsListener {
            override fun onItemClicked(fileInfo: FileInfo) {
                if (fileInfo.isDirectory()) {
                    path.offer(fileInfo.path)
                } else {
                    DownloadFileDialogFragment.newInstance(fileInfo).show(supportFragmentManager)
                }
            }

            override fun onItemLongClicked(fileInfo: FileInfo): Boolean {
                return if (fileInfo.type == FileInfo.FileType.FILE) {
                    FileMenuDialogFragment.newInstance(fileInfo).show(supportFragmentManager)

                    true
                } else {
                    false
                }
            }
        }

        ReconnectIssueDialogFragment.showIfNeeded(this)

        folder = intent.getStringExtra(EXTRA_FOLDER_NAME)
        path.offer(if (savedInstanceState == null) IndexBrowser.ROOT_PATH else savedInstanceState.getString(STATUS_PATH))

        launch {
            var job = Job()

            path.consumeEach { path ->
                job.cancel()
                job = Job()

                binding.listView.scrollToPosition(0)

                listing.send(null)

                async(job) {
                    libraryHandler.libraryManager.streamDirectoryListing(folder, path).consumeEach {
                        listing.send(it)
                    }
                }
            }
        }

        launch {
            listing.openSubscription().consumeEach { listing ->
                if (listing == null) {
                    binding.isLoading = true
                } else {
                    supportActionBar?.title = if (PathUtils.isRoot(listing.path)) folder else PathUtils.getFileName(listing.path)
                    binding.isLoading = false
                    adapter.data = if (listing is DirectoryContentListing)
                        listing.entries.sortedWith(IndexBrowser.sortAlphabeticallyDirectoriesFirst)
                    else
                        emptyList()
                }
            }
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)

        outState.putString(STATUS_PATH, path.value)
    }

    private fun goUp(): Boolean {
        val currentListing = listing.value

        val parentPath = when (currentListing) {
            is DirectoryContentListing -> currentListing.parentEntry?.path
            is DirectoryNotFoundListing -> currentListing.theoreticalParentPath
            else -> null
        }

        return if (parentPath == null) {
            false
        } else {
            path.offer(parentPath)

            true
        }
    }

    override fun onBackPressed() {
        if (!goUp()) {
            super.onBackPressed()
        }
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, intent: Intent?) {
        if (requestCode == REQUEST_SELECT_UPLOAD_FILE && resultCode == Activity.RESULT_OK) {
            libraryHandler.syncthingClient { syncthingClient ->
                GlobalScope.launch (Dispatchers.Main) {
                    // FIXME: it would be better if the dialog would use the library handler
                    FileUploadDialog(
                            this@FolderBrowserActivity,
                            syncthingClient,
                            intent!!.data,
                            folder,
                            path.value,
                            { /* nothing to do on success */ }
                    ).show()
                }
            }
        } else {
            super.onActivityResult(requestCode, resultCode, intent)
        }
    }
}
