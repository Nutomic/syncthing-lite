package net.syncthing.java.bep.index

import java.util.*

class FolderStatsUpdateCollector (val folderId: String) {
    var deltaFileCount = 0L
    var deltaDirCount = 0L
    var deltaSize = 0L
    var lastModified = Date(0)

    fun isEmpty() = (
            deltaFileCount == 0L &&
                    deltaDirCount == 0L &&
                    deltaSize == 0L &&
                    lastModified.time == 0L
            )
}
