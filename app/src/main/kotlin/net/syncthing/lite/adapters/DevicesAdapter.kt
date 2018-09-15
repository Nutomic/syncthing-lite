package net.syncthing.lite.adapters

import android.support.v7.widget.RecyclerView
import android.view.LayoutInflater
import android.view.ViewGroup
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.lite.databinding.ListviewDeviceBinding
import kotlin.properties.Delegates

class DevicesAdapter: RecyclerView.Adapter<DeviceViewHolder>() {
    var data: List<DeviceInfo> by Delegates.observable(listOf()) {
        _, _, _ -> notifyDataSetChanged()
    }

    var listener: DeviceAdapterListener? = null

    init {
        setHasStableIds(true)
    }

    override fun getItemCount() = data.size
    override fun getItemId(position: Int) = data[position].deviceId.deviceId.hashCode().toLong()

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = DeviceViewHolder(
            ListviewDeviceBinding.inflate(
                    LayoutInflater.from(parent.context), parent, false
            )
    )

    override fun onBindViewHolder(holder: DeviceViewHolder, position: Int) {
        val deviceStats = data[position]
        val binding = holder.binding

        binding.name = deviceStats.name
        binding.isConnected = deviceStats.isConnected

        binding.root.setOnLongClickListener { listener?.onDeviceLongClicked(deviceStats) ?: false }

        binding.executePendingBindings()
    }
}

interface DeviceAdapterListener {
    fun onDeviceLongClicked(deviceInfo: DeviceInfo): Boolean
}

class DeviceViewHolder(val binding: ListviewDeviceBinding): RecyclerView.ViewHolder(binding.root)