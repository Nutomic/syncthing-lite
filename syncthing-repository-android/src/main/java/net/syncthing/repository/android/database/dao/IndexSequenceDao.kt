package net.syncthing.repository.android.database.dao

import android.arch.persistence.room.Dao
import android.arch.persistence.room.Insert
import android.arch.persistence.room.Query
import net.syncthing.repository.android.database.item.IndexSequenceItem

@Dao
interface IndexSequenceDao {
    @Query("SELECT * FROM index_sequence")
    fun getItem(): IndexSequenceItem?

    @Insert
    fun createItem(item: IndexSequenceItem)

    @Query("UPDATE index_sequence SET current_sequence = current_sequence + 1 WHERE index_id = :indexId")
    fun incrementSequenceNumber(indexId: Long)
}
