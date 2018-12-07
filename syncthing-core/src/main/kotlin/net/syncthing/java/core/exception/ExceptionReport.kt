/*
 * Copyright 2018 Jonas Lochmann
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.exception

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlin.Exception

data class ExceptionReport(
        val component: String,
        val exception: Exception,
        val details: List<ExceptionDetails>
) {
    companion object {
        fun fromException(exception: Exception, component: String) = ExceptionReport(
                component,
                exception,
                ExceptionDetailException.getExceptionReportDetails(exception)
        )
    }

    val detailsReadableString: String by lazy {
        details.map { it.readableString }.joinToString("\n")
    }
}

data class ExceptionDetails(
        val component: String,
        val details: String
) {
    val readableString: String by lazy { component + "\n" + details + "\n" }
}

class ExceptionDetailException(
        cause: Throwable,
        val details: ExceptionDetails
): Exception(cause) {
    companion object {
        fun getExceptionReportDetails(exception: Exception): List<ExceptionDetails> {
            val result = mutableListOf<ExceptionDetails>()

            var ex: Throwable? = exception

            while (ex != null) {
                if (ex is ExceptionDetailException) {
                    result.add(ex.details)
                }

                ex = ex.cause
            }

            return result.reversed()
        }
    }
}

fun Job.reportExceptions(component: String, exceptionReportHandler: (ExceptionReport) -> Unit) {
    invokeOnCompletion {
        if (it != null) {
            if (it is Exception) {
                if (it is CancellationException) {
                    // ignore
                } else {
                    exceptionReportHandler(ExceptionReport.fromException(it, component))
                }
            } else {
                throw it
            }
        }
    }
}
