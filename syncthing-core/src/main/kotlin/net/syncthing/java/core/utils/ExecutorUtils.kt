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

import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(ExecutorService::class.java)

fun ExecutorService.awaitTerminationSafe() {
    try {
        awaitTermination(2, TimeUnit.SECONDS)
    } catch (ex: InterruptedException) {
        logger.warn("", ex)
    }
}

fun ExecutorService.submitLogging(runnable: Runnable) = submitLogging { runnable.run() }

/**
 * Wrapper method for [[ExecutorService.submit]], which silently swallows exceptions. If an exception is thrown in
 * [[runnable]], logs the exception and force crashes
 */
fun <T> ExecutorService.submitLogging(runnable: () -> T): Future<T> {
    return submit<T>({
        try {
            runnable()
        } catch (e: Exception) {
            logger.error("", e)
            System.exit(1)
            null
        }
    })
}

fun ExecutorService.trySubmitLogging(runnable: Runnable) {
    try {
        submitLogging(runnable)
    } catch (ex: RejectedExecutionException) {
        logger.warn("could not submit task", ex)
    }
}
