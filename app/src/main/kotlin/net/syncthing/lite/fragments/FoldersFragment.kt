package net.syncthing.lite.fragments

import android.arch.lifecycle.Observer
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.lite.activities.FolderBrowserActivity
import net.syncthing.lite.adapters.FolderListAdapterListener
import net.syncthing.lite.adapters.FoldersListAdapter
import net.syncthing.lite.databinding.FragmentFoldersBinding
import org.jetbrains.anko.intentFor

class FoldersFragment : SyncthingFragment() {
    override fun onCreateView(inflater: LayoutInflater, container: ViewGroup?, savedInstanceState: Bundle?): View? {
        val adapter = FoldersListAdapter()

        adapter.listener = object : FolderListAdapterListener {
            override fun onFolderClicked(folderInfo: FolderInfo, folderStats: FolderStats) {
                startActivity(
                        activity!!.intentFor<FolderBrowserActivity>(
                                FolderBrowserActivity.EXTRA_FOLDER_NAME to folderInfo.folderId
                        )
                )
            }
        }

        val binding = FragmentFoldersBinding.inflate(layoutInflater, container, false)
        binding.list.adapter = adapter
        libraryHandler.isListeningPortTaken.observe(this, Observer { binding.listeningPortTaken = it })

        launch {
            libraryHandler.subscribeToFolderStatusList().consumeEach {
                adapter.data = it
                binding.isEmpty = it.isEmpty()
            }
        }

        return binding.root
    }
}
