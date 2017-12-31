package net.syncthing.lite.adapters

import android.content.Context
import android.databinding.DataBindingUtil
import android.text.format.DateUtils
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
import com.google.common.collect.Lists
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ListviewFileBinding
import org.apache.commons.io.FileUtils

class FolderContentsAdapter(context: Context) :
        ArrayAdapter<FileInfo>(context, R.layout.listview_file, Lists.newArrayList()) {

    override fun getView(position: Int, v: View?, parent: ViewGroup): View {
        val binding: ListviewFileBinding =
            if (v == null) {
                DataBindingUtil.inflate(LayoutInflater.from(context), R.layout.listview_file, parent, false)
            } else {
                DataBindingUtil.bind(v)
            }
        val fileInfo = getItem(position)
        binding.fileLabel.text = fileInfo!!.fileName
        if (fileInfo.isDirectory) {
            binding.fileIcon.setImageResource(R.drawable.ic_folder_black_24dp)
            binding.fileSize.visibility = View.GONE
        } else {
            binding.fileIcon.setImageResource(R.drawable.ic_image_black_24dp)
            binding.fileSize.visibility = View.VISIBLE
            binding.fileSize.text = (FileUtils.byteCountToDisplaySize(fileInfo.size!!)
                    + context.getString(R.string.last_modified)
                    + DateUtils.getRelativeDateTimeString(context, fileInfo.lastModified.time, DateUtils.MINUTE_IN_MILLIS, DateUtils.WEEK_IN_MILLIS, 0))
        }
        return binding.root
    }
}
