/* 
 * Copyright (C) 2016 Davide Imbriaco
 * Copyright (C) 2018 Jonas Lochmann
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.utils

object PathUtils {
    const val ROOT_PATH = ""
    const val PATH_SEPARATOR = "/"
    const val PATH_SEPARATOR_WIN = "\\"
    const val PARENT_PATH = ".."
    const val CURRENT_PATH = "."

    fun isRoot(path: String): Boolean {
        return path.isEmpty()
    }

    private fun containsRelativeElements(path: String): Boolean {
        val pathSegments = path.split(PATH_SEPARATOR)

        return pathSegments.contains(PARENT_PATH) or pathSegments.contains(CURRENT_PATH)
    }

    private fun isTrimmed(value: String) = value.trim() == value
    private fun containsWindowsPathSeparator(path: String) = path.contains(PATH_SEPARATOR_WIN)
    private fun startsWithPathSeperator(path: String) = path.startsWith(PATH_SEPARATOR)
    private fun isValidPath(path: String) = (!containsRelativeElements(path)) and
            (!containsWindowsPathSeparator(path)) and
            path.isNotEmpty() and
            (!startsWithPathSeperator(path)) and
            isTrimmed(path)

    private fun containsPathSeparator(file: String) = file.contains(PATH_SEPARATOR) or file.contains(PATH_SEPARATOR_WIN)
    private fun isFilenameValid(file: String) = file.isNotBlank() and
            (!containsPathSeparator(file)) and
            isTrimmed(file)

    private fun assertPathValid(path: String) {
        if (!isValidPath(path)) {
            throw IllegalArgumentException("provided path is invalid")
        }
    }

    private fun assertFilenameValid(filename: String) {
        if (!isFilenameValid(filename)) {
            throw IllegalArgumentException("provided filename is invalid")
        }
    }

    fun isParent(path: String): Boolean {
        return path == PARENT_PATH
    }

    fun getParentPath(path: String): String {
        assertPathValid(path)

        val pathWithoutSuffix = path.removeSuffix(PATH_SEPARATOR)
        val previousSeparator = pathWithoutSuffix.lastIndexOf(PATH_SEPARATOR)

        return if (previousSeparator == -1) {
            ROOT_PATH
        } else {
            pathWithoutSuffix.substring(0, previousSeparator)
        }
    }

    fun getFileName(path: String): String {
        if (path.isEmpty()) {
            // this is required for IndexHandler.ROOT_FILE_INFO

            return ""
        }

        assertPathValid(path)

        val pathWithoutSuffix = path.removeSuffix(PATH_SEPARATOR)
        val previousSeparator = pathWithoutSuffix.lastIndexOf(PATH_SEPARATOR)

        return if (previousSeparator == -1) {
            // the file is in the root directory
            pathWithoutSuffix
        } else {
            pathWithoutSuffix.substring(previousSeparator + 1)
        }
    }

    fun buildPath(dir: String, file: String): String {
        assertPathValid(dir)
        assertFilenameValid(file)

        return dir.removeSuffix(PATH_SEPARATOR) + file
    }
}
