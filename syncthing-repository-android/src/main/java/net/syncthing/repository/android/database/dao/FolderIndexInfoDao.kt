package net.syncthing.repository.android.database.dao

import android.arch.persistence.room.*
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.repository.android.database.converters.DeviceIdConverter
import net.syncthing.repository.android.database.item.FolderIndexInfoItem

@Dao
@TypeConverters(DeviceIdConverter::class)
interface FolderIndexInfoDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    fun updateIndexInfo(item: FolderIndexInfoItem)

    @Query("SELECT * FROM folder_index_info WHERE device_id = :deviceId AND folder = :folder")
    fun findIndexInfoByDeviceAndFolder(deviceId: DeviceId, folder: String): FolderIndexInfoItem?

    @Query("SELECT * FROM folder_index_info")
    fun findAllIndexInfo(): List<FolderIndexInfoItem>
}
