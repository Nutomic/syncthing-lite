package net.syncthing.lite.adapters

import android.content.Context
import android.databinding.DataBindingUtil
import android.text.format.DateUtils
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ListviewFolderBinding
import org.apache.commons.lang3.tuple.Pair

class FoldersListAdapter(context: Context?, list: List<Pair<FolderInfo, FolderStats>>) :
        ArrayAdapter<Pair<FolderInfo, FolderStats>>(context, R.layout.listview_folder, list) {

    override fun getView(position: Int, v: View?, parent: ViewGroup): View {
        val binding: ListviewFolderBinding =
            if (v == null) {
                DataBindingUtil.inflate(LayoutInflater.from(context), R.layout.listview_folder, parent, false)
            } else {
                DataBindingUtil.bind(v)
            }
        val folderInfo = getItem(position)!!.left
        val folderStats = getItem(position)!!.right
        binding.folderName.text = "${folderInfo.label} (${folderInfo.folder})"
        binding.folderLastmodInfo.text =
                if (folderStats.lastUpdate == null)
                    context.getString(R.string.last_modified_unknown)
                else context.getString(R.string.last_modified_known) + " " +
                        DateUtils.getRelativeDateTimeString(context, folderStats.lastUpdate.time, DateUtils.MINUTE_IN_MILLIS, DateUtils.WEEK_IN_MILLIS, 0)
        binding.folderContentInfo.text = "${folderStats.describeSize()}, ${folderStats.fileCount} files, ${folderStats.dirCount} dirs"
        return binding.root
    }

}
