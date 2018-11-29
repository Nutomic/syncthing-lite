package net.syncthing.java.bep.index

import net.syncthing.java.core.beans.FolderStats

class FolderStatsUpdateCollector {
    private val updates = mutableMapOf<String, FolderStats>()

    fun put(folderStats: FolderStats) {
        updates[folderStats.folderId] = folderStats
    }

    fun query() = updates.values.toList()
}
