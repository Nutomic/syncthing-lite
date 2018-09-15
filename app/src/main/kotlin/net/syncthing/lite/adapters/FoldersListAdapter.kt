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

class FoldersListAdapter(context: Context?, list: List<Pair<FolderInfo, FolderStats>>) :
        ArrayAdapter<Pair<FolderInfo, FolderStats>>(context, R.layout.listview_folder, list) {

    override fun getView(position: Int, v: View?, parent: ViewGroup): View {
        val binding: ListviewFolderBinding =
            if (v == null) {
                DataBindingUtil.inflate(LayoutInflater.from(context), R.layout.listview_folder, parent, false)
            } else {
                DataBindingUtil.bind(v)!!
            }
        val folderInfo = getItem(position)!!.first
        val folderStats = getItem(position)!!.second
        binding.folderName.text = context.getString(R.string.folder_label_format, folderInfo.label, folderInfo.folderId)

        binding.folderLastmodInfo.text = context.getString(R.string.last_modified_time,
                        DateUtils.getRelativeDateTimeString(context, folderStats.lastUpdate.time, DateUtils.MINUTE_IN_MILLIS, DateUtils.WEEK_IN_MILLIS, 0))
        binding.folderContentInfo.text = context.getString(R.string.folder_content_info, folderStats.describeSize(), folderStats.fileCount, folderStats.dirCount)
        return binding.root
    }

}
