package net.syncthing.lite.adapters

import android.support.v7.widget.RecyclerView
import android.text.format.DateUtils
import android.util.Log
import android.view.LayoutInflater
import android.view.ViewGroup
import net.syncthing.java.bep.folder.FolderStatus
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.beans.FolderStats
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ListviewFolderBinding
import kotlin.properties.Delegates

class FoldersListAdapter: RecyclerView.Adapter<FolderListViewHolder>() {
    var data: List<FolderStatus> by Delegates.observable(listOf()) {
        _, _, _ -> notifyDataSetChanged()
    }

    var listener: FolderListAdapterListener? = null

    init {
        setHasStableIds(true)
    }

    override fun getItemCount() = data.size
    override fun getItemId(position: Int) = data[position].info.folderId.hashCode().toLong()

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = FolderListViewHolder (
            ListviewFolderBinding.inflate(
                    LayoutInflater.from(parent.context), parent, false
            )
    )

    override fun onBindViewHolder(holder: FolderListViewHolder, position: Int) {
        val binding = holder.binding
        val item = data[position]
        val (folderInfo, folderStats) = item
        val context = holder.itemView.context

        Log.d("FolderListAdapter", "$item")

        binding.folderName = context.getString(R.string.folder_label_format, folderInfo.label, folderInfo.folderId)

        binding.lastModification = context.getString(R.string.last_modified_time,
                DateUtils.getRelativeDateTimeString(context, folderStats.lastUpdate.time, DateUtils.MINUTE_IN_MILLIS, DateUtils.WEEK_IN_MILLIS, 0))

        binding.info = context.getString(R.string.folder_content_info, folderStats.sizeDescription, folderStats.fileCount, folderStats.dirCount)

        binding.info2 = if (item.missingIndexUpdates == 0L)
            null
        else
            context.getString(R.string.pending_index_updates, item.missingIndexUpdates)

        binding.root.setOnClickListener {
            listener?.onFolderClicked(folderInfo, folderStats)
        }

        binding.root.setOnLongClickListener {
            listener?.onFolderLongClicked(folderInfo) ?: false
        }
    }
}

class FolderListViewHolder(val binding: ListviewFolderBinding): RecyclerView.ViewHolder(binding.root)

interface FolderListAdapterListener {
    fun onFolderClicked(folderInfo: FolderInfo, folderStats: FolderStats)
    fun onFolderLongClicked(folderInfo: FolderInfo): Boolean
}
