package net.syncthing.lite.activities

import android.Manifest
import android.app.Activity
import android.app.AlertDialog
import android.content.Intent
import android.content.pm.PackageManager
import android.databinding.DataBindingUtil
import android.os.AsyncTask
import android.os.Bundle
import android.support.v4.app.ActivityCompat
import android.support.v4.content.ContextCompat
import android.util.Log
import android.view.View
import android.widget.Toast
import com.google.common.base.Objects.equal
import com.google.common.base.Preconditions.checkArgument
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.utils.FileInfoOrdering
import net.syncthing.java.core.utils.PathUtils
import net.syncthing.lite.R
import net.syncthing.lite.adapters.FolderContentsAdapter
import net.syncthing.lite.databinding.ActivityFolderBrowserBinding
import net.syncthing.lite.databinding.DialogLoadingBinding
import net.syncthing.lite.library.DownloadFileTask
import net.syncthing.lite.library.UploadFileTask
import org.jetbrains.anko.intentFor

class FolderBrowserActivity : SyncthingActivity() {

    companion object {

        private val TAG = "FolderBrowserActivity"
        private val REQUEST_WRITE_STORAGE = 142
        private val REQUEST_SELECT_UPLOAD_FILE = 171

        val EXTRA_FOLDER_NAME = "folder_name"
    }

    private lateinit var binding: ActivityFolderBrowserBinding
    private var indexBrowser: IndexBrowser? = null
    private var loadingDialog: AlertDialog? = null
    private var adapter: FolderContentsAdapter? = null
    private var runWhenPermissionsReceived: Runnable? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = DataBindingUtil.setContentView(this, R.layout.activity_folder_browser)
        binding.mainListViewUploadHereButton.setOnClickListener { showUploadHereDialog() }
        showFolderListView(intent.getStringExtra(EXTRA_FOLDER_NAME), null)
    }

    override fun onDestroy() {
        super.onDestroy()
        Thread {
            indexBrowser?.close()
            indexBrowser = null
        }.start()
        cancelLoadingDialog()
    }

    override fun onBackPressed() {
        val listView = binding.mainFolderAndFilesListView
        //click item '0', ie '..' (go to parent)
        listView.performItemClick(adapter!!.getView(0, null, listView), 0, listView.getItemIdAtPosition(0))
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, intent: Intent?) {
        if (requestCode == REQUEST_SELECT_UPLOAD_FILE && resultCode == Activity.RESULT_OK) {
            UploadFileTask(this, syncthingClient(), intent!!.data, indexBrowser!!.folder,
                    indexBrowser!!.currentPath, { this.updateFolderListView() }).uploadFile()
        }
    }

    private fun showLoadingDialog(message: String) {
        val binding = DataBindingUtil.inflate<DialogLoadingBinding>(
                layoutInflater, R.layout.dialog_loading, null, false)
        binding.loadingText.text = message
        loadingDialog = android.app.AlertDialog.Builder(this)
                .setCancelable(false)
                .setView(binding.root)
                .show()
    }

    private fun cancelLoadingDialog() {
        loadingDialog?.cancel()
        loadingDialog = null
    }

    private fun showFolderListView(folder: String, previousPath: String?) {
        if (indexBrowser != null && equal(folder, indexBrowser!!.folder)) {
            Log.d(TAG, "reuse current index browser")
            indexBrowser!!.navigateToNearestPath(previousPath)
        } else {
            if (indexBrowser != null) {
                indexBrowser!!.close()
            }
            Log.d(TAG, "open new index browser")
            indexBrowser = syncthingClient().indexHandler
                    .newIndexBrowserBuilder()
                    .setOrdering(FileInfoOrdering.ALPHA_ASC_DIR_FIRST)
                    .includeParentInList(true).allowParentInRoot(true)
                    .setFolder(folder)
                    .buildToNearestPath(previousPath)
        }
        adapter = FolderContentsAdapter(this)
        binding.mainFolderAndFilesListView.adapter = adapter
        binding.mainFolderAndFilesListView.setOnItemClickListener { _, _, position, _ ->
            val fileInfo = binding.mainFolderAndFilesListView.getItemAtPosition(position) as FileInfo
            Log.d(TAG, "navigate to path = '" + fileInfo.path + "' from path = '" + indexBrowser!!.currentPath + "'")
            navigateToFolder(fileInfo)
        }
        navigateToFolder(indexBrowser!!.currentPathInfo)
    }

    private fun navigateToFolder(fileInfo: FileInfo) {
        if (indexBrowser!!.isRoot && PathUtils.isParent(fileInfo.path)) {
            finish()
        } else {
            if (fileInfo.isDirectory) {
                indexBrowser!!.navigateTo(fileInfo)
                val newFileInfo = if (PathUtils.isParent(fileInfo.path)) indexBrowser!!.currentPathInfo else fileInfo
                if (!indexBrowser!!.isCacheReadyAfterALittleWait) {
                    Log.d(TAG, "load folder cache bg")
                    object : AsyncTask<Void?, Void?, Void?>() {
                        override fun onPreExecute() {
                            // TODO: show ProgressBar in ListView instead of dialog
                            showLoadingDialog(getString(R.string.open_directory) +
                                    if (indexBrowser!!.isRoot) folderBrowser()?.getFolderInfo(indexBrowser!!.folder)?.label
                                    else indexBrowser!!.currentPathFileName)
                        }

                        override fun doInBackground(vararg voids: Void?): Void? {
                            indexBrowser!!.waitForCacheReady()
                            return null
                        }

                        override fun onPostExecute(aVoid: Void?) {
                            Log.d(TAG, "cache ready, navigate to folder")
                            cancelLoadingDialog()
                            navigateToFolder(newFileInfo)
                        }
                    }.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR)
                } else {
                    val list = indexBrowser!!.listFiles()
                    Log.i("navigateToFolder", "list for path = '" + indexBrowser!!.currentPath + "' list = " + list.size + " records")
                    Log.d("navigateToFolder", "list for path = '" + indexBrowser!!.currentPath + "' list = " + list)
                    checkArgument(!list.isEmpty())//list must contain at least the 'parent' path
                    adapter!!.clear()
                    adapter!!.addAll(list)
                    adapter!!.notifyDataSetChanged()
                    binding.mainFolderAndFilesListView.setSelection(0)
                    supportActionBar!!.setTitle(if (indexBrowser!!.isRoot)
                        folderBrowser()?.getFolderInfo(indexBrowser!!.folder)?.label
                    else
                        newFileInfo.fileName)
                }
            } else {
                Log.i(TAG, "pulling file = " + fileInfo)
                executeWithPermissions(
                        Runnable { DownloadFileTask(this, syncthingClient(), fileInfo).downloadFile() })
            }
        }
    }

    private fun updateFolderListView() {
        showFolderListView(indexBrowser!!.folder, indexBrowser!!.currentPath)
    }

    private fun showUploadHereDialog() {
        executeWithPermissions(Runnable {
            startActivityForResult(intentFor<FilePickerActivity>(), REQUEST_SELECT_UPLOAD_FILE)
        })
    }

    override fun onIndexUpdateProgress(folder: FolderInfo, percentage: Int) {
        binding.mainIndexProgressBarLabel.text = (getString(R.string.index_update_folder)
                + folder.label + " " + percentage + getString(R.string.index_update_percent_synchronized))
        updateFolderListView()
    }

    override fun onIndexUpdateComplete() {
        binding.mainIndexProgressBar.visibility = View.GONE
        updateFolderListView()
    }

    private fun executeWithPermissions(runnable: Runnable) {
        val permissionState = ContextCompat.checkSelfPermission(this,
                Manifest.permission.WRITE_EXTERNAL_STORAGE)
        if (permissionState != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this,
                    arrayOf(Manifest.permission.WRITE_EXTERNAL_STORAGE),
                    REQUEST_WRITE_STORAGE)
            runWhenPermissionsReceived = runnable
        } else {
            runnable.run()
        }
    }

    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<String>,
                                            grantResults: IntArray) {
        when (requestCode) {
            REQUEST_WRITE_STORAGE -> {
                if (grantResults.isEmpty() || grantResults[0] != PackageManager.PERMISSION_GRANTED) {
                    Toast.makeText(this, R.string.toast_write_storage_permission_required,
                            Toast.LENGTH_LONG).show()
                } else {
                    runWhenPermissionsReceived!!.run()
                }
                runWhenPermissionsReceived = null
            }
            else -> super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        }
    }
}
