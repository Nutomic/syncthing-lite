package net.syncthing.java.bep.index

import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.IndexInfo

sealed class IndexInfoUpdateEvent
data class IndexRecordAcquiredEvent(val folderId: String, val files: List<FileInfo>, val indexInfo: IndexInfo): IndexInfoUpdateEvent()
object IndexInfoClearedEvent: IndexInfoUpdateEvent()
