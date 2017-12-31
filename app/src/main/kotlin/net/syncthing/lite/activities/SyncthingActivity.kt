package net.syncthing.lite.activities

import android.app.AlertDialog
import android.databinding.DataBindingUtil
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.view.LayoutInflater
import net.syncthing.java.bep.FolderBrowser
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.lite.BuildConfig
import net.syncthing.lite.R
import net.syncthing.lite.databinding.DialogLoadingBinding
import net.syncthing.lite.library.InitLibraryTask
import net.syncthing.lite.library.LibraryHandler
import org.slf4j.impl.HandroidLoggerAdapter

abstract class SyncthingActivity : AppCompatActivity() {

    companion object {
        private var activityCount = 0
        private var libraryHandler: LibraryHandler? = null
    }

    private var loadingDialog: AlertDialog? = null

    fun syncthingClient(): SyncthingClient = libraryHandler!!.syncthingClient!!

    fun configuration(): ConfigurationService = libraryHandler!!.configuration!!

    fun folderBrowser(): FolderBrowser? = libraryHandler?.folderBrowser

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        HandroidLoggerAdapter.DEBUG = BuildConfig.DEBUG
        activityCount++
        if (libraryHandler == null) {
            val binding = DataBindingUtil.inflate<DialogLoadingBinding>(
                    LayoutInflater.from(this), R.layout.dialog_loading, null, false)
            binding.loadingText.text = getString(R.string.loading_config_starting_syncthing_client)
            loadingDialog = AlertDialog.Builder(this)
                    .setCancelable(false)
                    .setView(binding.root)
                    .show()
            InitLibraryTask(this, this::onLibraryLoaded, this::onIndexUpdateProgress, this::onIndexUpdateComplete)
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        activityCount--
        Thread {
            if (activityCount == 0) {
                libraryHandler?.destroy()
                libraryHandler = null
            }
        }.start()
    }

    private fun onLibraryLoaded(libraryHandler: LibraryHandler) {
        if (activityCount == 0)
            return

        SyncthingActivity.libraryHandler = libraryHandler
        loadingDialog!!.cancel()
        onLibraryLoaded()
    }

    open fun onIndexUpdateProgress(folder: FolderInfo, percentage: Int) {}

    open fun onIndexUpdateComplete() {}

    open fun onLibraryLoaded() {}
}
