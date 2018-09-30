package net.syncthing.repository.android.database.converters

import android.arch.persistence.room.TypeConverter
import java.util.*

class DateConverter {
    @TypeConverter
    fun toLong(date: Date) = date.time

    @TypeConverter
    fun fromLong(time: Long) = Date(time)
}
