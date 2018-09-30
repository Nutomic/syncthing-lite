package net.syncthing.repository.android.database.converters

import android.arch.persistence.room.TypeConverter
import net.syncthing.java.core.beans.FileInfo

class FileTypeConverter {
    companion object {
        private const val FILE = "file"
        private const val DIRECTORY = "directory"
    }

    @TypeConverter
    fun toString(type: FileInfo.FileType) = when (type) {
        FileInfo.FileType.DIRECTORY -> DIRECTORY
        FileInfo.FileType.FILE -> FILE
    }

    @TypeConverter
    fun fromString(value: String)  = when (value) {
        FILE -> FileInfo.FileType.FILE
        DIRECTORY -> FileInfo.FileType.DIRECTORY
        else -> throw IllegalArgumentException()
    }
}
