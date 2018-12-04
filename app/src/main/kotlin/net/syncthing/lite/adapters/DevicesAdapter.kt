package net.syncthing.lite.adapters

import android.support.v7.widget.RecyclerView
import android.view.LayoutInflater
import android.view.ViewGroup
import net.syncthing.java.bep.connectionactor.ConnectionInfo
import net.syncthing.java.bep.connectionactor.ConnectionStatus
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.lite.R
import net.syncthing.lite.databinding.ListviewDeviceBinding
import kotlin.properties.Delegates

class DevicesAdapter: RecyclerView.Adapter<DeviceViewHolder>() {
    var data: List<Pair<DeviceInfo, ConnectionInfo>> by Delegates.observable(listOf()) {
        _, _, _ -> notifyDataSetChanged()
    }

    var listener: DeviceAdapterListener? = null

    init {
        setHasStableIds(true)
    }

    override fun getItemCount() = data.size
    override fun getItemId(position: Int) = data[position].first.deviceId.deviceId.hashCode().toLong()

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = DeviceViewHolder(
            ListviewDeviceBinding.inflate(
                    LayoutInflater.from(parent.context), parent, false
            )
    )

    override fun onBindViewHolder(holder: DeviceViewHolder, position: Int) {
        val binding = holder.binding
        val context = binding.root.context
        val (deviceInfo, connectionInfo) = data[position]

        binding.name = deviceInfo.name
        binding.isConnected = connectionInfo.status == ConnectionStatus.Connected

        binding.status = when (connectionInfo.status) {
            ConnectionStatus.Connected -> context.getString(R.string.device_status_connected, connectionInfo.currentAddress?.address)
            ConnectionStatus.Connecting -> context.getString(R.string.device_status_connecting, connectionInfo.currentAddress?.address)
            ConnectionStatus.Disconnected -> if (connectionInfo.addresses.isEmpty())
                context.getString(R.string.device_status_no_address)
            else
                context.getString(R.string.device_status_disconnected, connectionInfo.addresses.size)
        }

        binding.root.setOnLongClickListener { listener?.onDeviceLongClicked(deviceInfo) ?: false }

        binding.executePendingBindings()
    }
}

interface DeviceAdapterListener {
    fun onDeviceLongClicked(deviceInfo: DeviceInfo): Boolean
}

class DeviceViewHolder(val binding: ListviewDeviceBinding): RecyclerView.ViewHolder(binding.root)
