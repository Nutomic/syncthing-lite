package net.syncthing.lite.fragments

import net.syncthing.lite.async.CoroutineDialogFragment
import net.syncthing.lite.library.LibraryHandler

abstract class SyncthingDialogFragment : CoroutineDialogFragment() {
    val libraryHandler: LibraryHandler by lazy { LibraryHandler(
            context = context!!
    )}

    override fun onStart() {
        super.onStart()

        libraryHandler.start()
    }

    override fun onStop() {
        super.onStop()

        libraryHandler.stop()
    }
}
