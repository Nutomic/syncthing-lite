package net.syncthing.lite.library

import android.arch.lifecycle.LiveData
import android.arch.lifecycle.MutableLiveData
import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import net.syncthing.java.bep.FolderBrowser
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.configuration.Configuration
import org.jetbrains.anko.doAsync
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class helps when using the library.
 * It's required to start and stop it to make the callbacks fire (or stop to fire).
 *
 * It's possible to do multiple start and stop cycles with one instance of this class.
 */
class LibraryHandler(context: Context,
                     private val onIndexUpdateProgressListener: (FolderInfo, Int) -> Unit = {_, _ -> },
                     private val onIndexUpdateCompleteListener: (FolderInfo) -> Unit = {}) {

    companion object {
        private const val TAG = "LibraryHandler"
        private val handler = Handler(Looper.getMainLooper())
    }

    private val libraryManager = DefaultLibraryManager.with(context)
    private val isStarted = AtomicBoolean(false)
    private val isListeningPortTakenInternal = MutableLiveData<Boolean>().apply { postValue(false) }

    val isListeningPortTaken: LiveData<Boolean> = isListeningPortTakenInternal

    private val messageFromUnknownDeviceListeners = HashSet<(DeviceId) -> Unit>()
    private val internalMessageFromUnknownDeviceListener: (DeviceId) -> Unit = {
        deviceId ->

        handler.post {
            messageFromUnknownDeviceListeners.forEach { listener -> listener(deviceId) }
        }
    }

    fun start(onLibraryLoaded: (LibraryHandler) -> Unit = {}) {
        if (isStarted.getAndSet(true) == true) {
            throw IllegalStateException("already started")
        }

        libraryManager.startLibraryUsage {
            libraryInstance ->

            isListeningPortTakenInternal.value = libraryInstance.isListeningPortTaken
            onLibraryLoaded(this)

            val client = libraryInstance.syncthingClient

            client.indexHandler.registerOnIndexRecordAcquiredListener(this::onIndexRecordAcquired)
            client.indexHandler.registerOnFullIndexAcquiredListenersListener(this::onRemoteIndexAcquired)
            client.discoveryHandler.registerMessageFromUnknownDeviceListener(internalMessageFromUnknownDeviceListener)
        }
    }

    fun stop() {
        if (isStarted.getAndSet(false) == false) {
            throw IllegalStateException("already stopped")
        }

        syncthingClient {
            try {
                it.indexHandler.unregisterOnIndexRecordAcquiredListener(this::onIndexRecordAcquired)
                it.indexHandler.unregisterOnFullIndexAcquiredListenersListener(this::onRemoteIndexAcquired)
                it.discoveryHandler.unregisterMessageFromUnknownDeviceListener(internalMessageFromUnknownDeviceListener)
            } catch (e: IllegalArgumentException) {
                // ignored, no idea why this is thrown
            }
        }

        libraryManager.stopLibraryUsage()
    }

    private fun onIndexRecordAcquired(folderInfo: FolderInfo, newRecords: List<FileInfo>, indexInfo: IndexInfo) {
        Log.i(TAG, "handleIndexRecordEvent trigger folder list update from index record acquired")

        GlobalScope.launch (Dispatchers.Main) {
            onIndexUpdateProgressListener(folderInfo, (indexInfo.getCompleted() * 100).toInt())
        }
    }

    private fun onRemoteIndexAcquired(folderInfo: FolderInfo) {
        Log.i(TAG, "handleIndexAcquiredEvent trigger folder list update from index acquired")

        GlobalScope.launch (Dispatchers.Main) {
            onIndexUpdateCompleteListener(folderInfo)
        }
    }

    /*
     * The callback is executed asynchronously.
     * As soon as it returns, there is no guarantee about the availability of the library
     */
    fun library(callback: (Configuration, SyncthingClient, FolderBrowser) -> Unit) {
        libraryManager.startLibraryUsage {
            doAsync {
                try {
                    callback(it.configuration, it.syncthingClient, it.folderBrowser)
                } finally {
                    libraryManager.stopLibraryUsage()
                }
            }
        }
    }

    fun syncthingClient(callback: (SyncthingClient) -> Unit) {
        library { _, s, _ -> callback(s) }
    }

    fun configuration(callback: (Configuration) -> Unit) {
        library { c, _, _ -> callback(c) }
    }

    fun folderBrowser(callback: (FolderBrowser) -> Unit) {
        library { _, _, f -> callback(f) }
    }

    // these listeners are called at the UI Thread
    // there is no need to unregister because they removed from the library when close is called
    fun registerMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        messageFromUnknownDeviceListeners.add(listener)
    }

    fun unregisterMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        messageFromUnknownDeviceListeners.remove(listener)
    }
}
