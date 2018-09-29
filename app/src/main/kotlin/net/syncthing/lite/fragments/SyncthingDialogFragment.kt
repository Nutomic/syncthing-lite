package net.syncthing.lite.fragments

import android.support.v4.app.DialogFragment
import net.syncthing.lite.library.LibraryHandler

abstract class SyncthingDialogFragment : DialogFragment() {
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
