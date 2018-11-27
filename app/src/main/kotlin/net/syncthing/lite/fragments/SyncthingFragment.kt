package net.syncthing.lite.fragments

import android.support.v4.app.Fragment
import net.syncthing.lite.library.LibraryHandler

abstract class SyncthingFragment : Fragment() {
    val libraryHandler: LibraryHandler by lazy { LibraryHandler(context = context!!)}

    override fun onStart() {
        super.onStart()

        libraryHandler.start {
            // TODO: check if this is still useful
            onLibraryLoaded()
        }
    }

    override fun onStop() {
        super.onStop()

        libraryHandler.stop()
    }

    open fun onLibraryLoaded() {}
}
