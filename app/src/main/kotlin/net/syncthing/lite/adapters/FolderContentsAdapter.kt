package net.syncthing.lite.adapters

import android.support.v7.widget.RecyclerView
import android.text.format.DateUtils
import android.view.LayoutInflater
import android.view.ViewGroup
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ListviewFileBinding
import org.apache.commons.io.FileUtils
import kotlin.properties.Delegates

// TODO: enable setHasStableIds and add a good way to get an id
class FolderContentsAdapter: RecyclerView.Adapter<FolderContentsViewHolder>() {
    var data: List<FileInfo> by Delegates.observable(listOf()) {
        _, _, _ -> notifyDataSetChanged()
    }

    var listener: FolderContentsListener? = null

    init {
        // setHasStableIds(true)
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = FolderContentsViewHolder(
            ListviewFileBinding.inflate(
                    LayoutInflater.from(parent.context),
                    parent,
                    false
            )
    )

    override fun onBindViewHolder(holder: FolderContentsViewHolder, position: Int) {
        val binding = holder.binding
        val fileInfo = data[position]

        binding.fileName = fileInfo.fileName

        if (fileInfo.isDirectory()) {
            binding.fileIcon.setImageResource(R.drawable.ic_folder_black_24dp)
            binding.fileSize = null
        } else {
            binding.fileIcon.setImageResource(R.drawable.ic_image_black_24dp)
            binding.fileSize = binding.root.context.getString(R.string.file_info,
                    FileUtils.byteCountToDisplaySize(fileInfo.size!!),
                    DateUtils.getRelativeDateTimeString(binding.root.context, fileInfo.lastModified.time, DateUtils.MINUTE_IN_MILLIS, DateUtils.WEEK_IN_MILLIS, 0))
        }

        binding.root.setOnClickListener {
            listener?.onItemClicked(fileInfo)
        }

        binding.root.setOnLongClickListener {
            listener?.onItemLongClicked(fileInfo) ?: false
        }

        binding.executePendingBindings()
    }

    override fun getItemCount() = data.size
    // override fun getItemId(position: Int) = data[position].fileName.hashCode().toLong()
}

interface FolderContentsListener {
    fun onItemClicked(fileInfo: FileInfo)
    fun onItemLongClicked(fileInfo: FileInfo): Boolean
}

class FolderContentsViewHolder(val binding: ListviewFileBinding): RecyclerView.ViewHolder(binding.root)
