package net.syncthing.lite.library

import android.content.Context
import android.util.Log
import net.syncthing.java.client.SyncthingClient
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.repository.android.SqliteIndexRepository
import net.syncthing.repository.android.TempDirectoryLocalRepository
import net.syncthing.repository.android.database.RepositoryDatabase
import org.jetbrains.anko.defaultSharedPreferences
import java.io.File
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketException

/**
 * This class is used internally to access the syncthing-java library
 * There should be never more than 1 instance of this class
 *
 * This class can not be recycled. This means that after doing a shutdown of it,
 * a new instance must be created
 *
 * The creation and the shutdown are synchronous, so keep them out of the UI Thread
 */
class LibraryInstance (context: Context) {
    companion object {
        private const val LOG_TAG = "LibraryInstance"

        /**
         * Check if listening port for local discovery is taken by another app. Do this check here to
         * avoid adding another callback.
         */
        private fun checkIsListeningPortTaken(): Boolean {
            try {
                DatagramSocket(21027, InetAddress.getByName("0.0.0.0")).close()

                return false
            } catch (e: SocketException) {
                Log.w(LOG_TAG, e)

                return true
            }
        }
    }

    private val tempRepository = TempDirectoryLocalRepository(File(context.filesDir, "temp_repository"))

    val isListeningPortTaken = checkIsListeningPortTaken()  // this must come first to work correctly
    val configuration = Configuration(configFolder = context.filesDir)
    val syncthingClient = SyncthingClient(
            configuration = configuration,
            repository = SqliteIndexRepository(
                    database = RepositoryDatabase.with(context),
                    closeDatabaseOnClose = false,
                    clearTempStorageHook = { tempRepository.deleteAllData() }
            ),
            tempRepository = tempRepository,
            enableDetailedException = context.defaultSharedPreferences.getBoolean("detailed_exception", false)
    )
    val folderBrowser = syncthingClient.indexHandler.folderBrowser
    val indexBrowser = syncthingClient.indexHandler.indexBrowser

    suspend fun shutdown() {
        syncthingClient.close()
        configuration.persistNow()
    }
}
