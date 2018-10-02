/* 
 * Copyright (C) 2016 Davide Imbriaco
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

import org.apache.commons.io.FilenameUtils

object PathUtils {

    val ROOT_PATH = ""
    val PATH_SEPARATOR = "/"
    val PARENT_PATH = ".."

    private fun normalizePath(path: String): String {
        return FilenameUtils.normalizeNoEndSeparator(path, true).replaceFirst(("^" + PATH_SEPARATOR).toRegex(), "")
    }

    fun isRoot(path: String): Boolean {
        return path.isEmpty()
    }

    fun isParent(path: String): Boolean {
        return path == PARENT_PATH
    }

    fun getParentPath(path: String): String {
        assert(!isRoot(path), {"cannot get parent of root path"})
        return normalizePath(path + PATH_SEPARATOR + PARENT_PATH)
    }

    fun getFileName(path: String): String {
        return FilenameUtils.getName(path)
    }

    fun buildPath(dir: String, file: String): String {
        return normalizePath(dir + PATH_SEPARATOR + file)
    }
}
