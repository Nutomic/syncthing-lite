package net.syncthing.repository.android.database.item

import android.arch.persistence.room.ColumnInfo
import android.arch.persistence.room.Entity
import android.arch.persistence.room.PrimaryKey

@Entity(
        tableName = "index_sequence"
)
data class IndexSequenceItem(
        @PrimaryKey
        @ColumnInfo(name = "index_id")
        val indexId: Long,
        @ColumnInfo(name = "current_sequence")
        val currentSequence: Long
)
