package net.syncthing.java.bep.index.browser

import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.utils.PathUtils

sealed class DirectoryListing {
    abstract val folder: String
    abstract val path: String
}

data class DirectoryContentListing(
        val directoryInfo: FileInfo,
        val parentEntry: FileInfo?,
        val entries: List<FileInfo>
): DirectoryListing() {
    override val folder = directoryInfo.folder
    override val path = directoryInfo.path
}

data class DirectoryNotFoundListing(
        override val folder: String,
        override val path: String
): DirectoryListing() {
    val theoreticalParentPath = if (PathUtils.isRoot(path)) null else PathUtils.getParentPath(path)
}
