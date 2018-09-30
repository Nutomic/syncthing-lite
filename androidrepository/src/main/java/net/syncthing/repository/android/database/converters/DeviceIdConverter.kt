package net.syncthing.repository.android.database.converters

import android.arch.persistence.room.TypeConverter
import net.syncthing.java.core.beans.DeviceId

class DeviceIdConverter {
    @TypeConverter
    fun fromString(deviceId: String) = DeviceId(deviceId)

    @TypeConverter
    fun toString(deviceId: DeviceId) = deviceId.deviceId
}
