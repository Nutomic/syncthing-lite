package net.syncthing.lite.library

import android.content.Context
import android.os.Handler
import android.util.Log
import kotlinx.coroutines.experimental.android.UI
import kotlinx.coroutines.experimental.async
import net.syncthing.java.bep.FolderBrowser
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.IndexInfo
import net.syncthing.java.core.configuration.Configuration
import org.jetbrains.anko.doAsync
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketException
import java.util.*

class LibraryHandler(context: Context, onLibraryLoaded: (LibraryHandler) -> Unit,
                     private val onIndexUpdateProgressListener: (FolderInfo, Int) -> Unit,
                     private val onIndexUpdateCompleteListener: (FolderInfo) -> Unit) {

    companion object {
        private var instanceCount = 0
        private var configuration: Configuration? = null
        private var syncthingClient: SyncthingClient? = null
        private var folderBrowser: FolderBrowser? = null
        private val callbacks = ArrayList<(Configuration, SyncthingClient, FolderBrowser) -> Unit>()
        private var isLoading = false
        var isListeningPortTaken = false
    }

    private val TAG = "LibraryHandler"

    init {
        instanceCount++
        if (configuration == null && !isLoading) {
            isLoading = true
            doAsync {
                checkIsListeningPortTaken()
                init(context)
                async(UI) {
                    onLibraryLoaded(this@LibraryHandler)
                }
                isLoading = false
            }
        } else {
            onLibraryLoaded(this)
        }

        syncthingClient {
            it.indexHandler.registerOnIndexRecordAcquiredListener(this::onIndexRecordAcquired)
            it.indexHandler.registerOnFullIndexAcquiredListenersListener(this::onRemoteIndexAcquired)
        }
    }

    private fun onIndexRecordAcquired(folderInfo: FolderInfo, newRecords: List<FileInfo>, indexInfo: IndexInfo) {
        Log.i(TAG, "handleIndexRecordEvent trigger folder list update from index record acquired")

        async(UI) {
            onIndexUpdateProgressListener(folderInfo, (indexInfo.getCompleted() * 100).toInt())
        }
    }

    private fun onRemoteIndexAcquired(folderInfo: FolderInfo) {
        Log.i(TAG, "handleIndexAcquiredEvent trigger folder list update from index acquired")

        async(UI) {
            onIndexUpdateCompleteListener(folderInfo)
        }
    }

    private fun init(context: Context) {
        val configuration = Configuration(configFolder = context.filesDir)
        val syncthingClient = SyncthingClient(configuration)
        val folderBrowser = syncthingClient.indexHandler.newFolderBrowser()

        if (instanceCount == 0) {
            Log.d(TAG, "All LibraryHandler instances were closed during init")
            syncthingClient.close()
            folderBrowser.close()
        }

        async(UI) {
            callbacks.forEach { it(configuration, syncthingClient, folderBrowser) }
        }
        LibraryHandler.configuration = configuration
        LibraryHandler.syncthingClient = syncthingClient
        LibraryHandler.folderBrowser = folderBrowser
    }

    fun library(callback: (Configuration, SyncthingClient, FolderBrowser) -> Unit) {
        val nullCount = listOf(configuration, syncthingClient, folderBrowser).count { it == null }
        assert(nullCount == 0 || nullCount == 3, { "Inconsistent library state" })

        // https://stackoverflow.com/a/35522422/1837158
        fun <T1: Any, T2: Any, T3: Any, R: Any> safeLet(p1: T1?, p2: T2?, p3: T3?, block: (T1, T2, T3)->R?): R? {
            return if (p1 != null && p2 != null && p3 != null) block(p1, p2, p3) else null
        }
        safeLet(configuration, syncthingClient, folderBrowser) { c, s, f ->
            callback(c, s, f)
        } ?: run {
            if (isLoading) {
                callbacks.add(callback)
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

    /**
     * Check if listening port for local discovery is taken by another app. Do this check here to
     * avoid adding another callback.
     */
    private fun checkIsListeningPortTaken() {
        try {
            DatagramSocket(21027, InetAddress.getByName("0.0.0.0")).close()
        } catch (e: SocketException) {
            Log.w(TAG, e)
            isListeningPortTaken = true
        }
    }

    /**
     * Unregisters index update listener and decreases instance count.
     *
     * We wait a bit before closing [[syncthingClient]] etc, in case LibraryHandler is opened again
     * soon (eg in case of device rotation).
     */
    fun close() {
        syncthingClient {
            try {
                it.indexHandler.unregisterOnIndexRecordAcquiredListener(this::onIndexRecordAcquired)
                it.indexHandler.unregisterOnFullIndexAcquiredListenersListener(this::onRemoteIndexAcquired)
            } catch (e: IllegalArgumentException) {
                // ignored, no idea why this is thrown
            }
        }

        instanceCount--
        Handler().postDelayed({
            Thread {
                if (instanceCount == 0) {
                    folderBrowser?.close()
                    folderBrowser = null
                    syncthingClient?.close()
                    syncthingClient = null
                    configuration = null
                }
            }.start()
        }, 60 * 1000)

    }

    fun registerMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        library { _, syncthingClient, _ ->
            syncthingClient.discoveryHandler.registerMessageFromUnknownDeviceListener(listener)
        }
    }

    fun unregisterMessageFromUnknownDeviceListener(listener: (DeviceId) -> Unit) {
        library { _, syncthingClient, _ ->
            syncthingClient.discoveryHandler.unregisterMessageFromUnknownDeviceListener(listener)
        }
    }
}
