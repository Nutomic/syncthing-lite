package net.syncthing.lite.fragments

import android.databinding.DataBindingUtil
import android.os.Bundle
import android.util.Log
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import kotlinx.coroutines.experimental.android.UI
import kotlinx.coroutines.experimental.async
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.lite.R
import net.syncthing.lite.activities.FolderBrowserActivity
import net.syncthing.lite.adapters.FolderListAdapterListener
import net.syncthing.lite.adapters.FoldersListAdapter
import net.syncthing.lite.databinding.FragmentFoldersBinding
import org.jetbrains.anko.intentFor

class FoldersFragment : SyncthingFragment() {

    private val TAG = "FoldersFragment"

    private lateinit var binding: FragmentFoldersBinding

    override fun onCreateView(inflater: LayoutInflater, container: ViewGroup?,
                              savedInstanceState: Bundle?): View? {
        binding = DataBindingUtil.inflate(layoutInflater, R.layout.fragment_folders, container, false)
        return binding.root
    }

    override fun onLibraryLoaded() {
        showAllFoldersListView()
    }

    private fun showAllFoldersListView() {
        libraryHandler.folderBrowser { folderBrowser ->
            val list = folderBrowser.folderInfoAndStatsList()

            async (UI) {
                Log.i(TAG, "list folders = " + list + " (" + list.size + " records)")
                val adapter = FoldersListAdapter().apply { data = list }
                binding.list.adapter = adapter
                adapter.listener = object : FolderListAdapterListener {
                    override fun onFolderClicked(folderInfo: FolderInfo, folderStats: FolderStats) {
                        startActivity(
                                activity!!.intentFor<FolderBrowserActivity>(
                                        FolderBrowserActivity.EXTRA_FOLDER_NAME to folderInfo.folderId
                                )
                        )
                    }
                }

                binding.isEmpty = list.isEmpty()
            }
        }
    }

    override fun onIndexUpdateComplete(folderInfo: FolderInfo) {
        super.onIndexUpdateComplete(folderInfo)
        showAllFoldersListView()
    }
}
