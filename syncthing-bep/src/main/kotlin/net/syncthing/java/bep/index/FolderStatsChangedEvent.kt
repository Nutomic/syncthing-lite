package net.syncthing.java.bep.index

import net.syncthing.java.core.beans.FolderStats

sealed class FolderStatsChangedEvent
data class FolderStatsUpdatedEvent(val folderStats: FolderStats): FolderStatsChangedEvent()
object FolderStatsResetEvent: FolderStatsChangedEvent()
