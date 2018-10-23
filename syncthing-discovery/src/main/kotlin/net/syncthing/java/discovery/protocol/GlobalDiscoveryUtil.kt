package net.syncthing.java.discovery.protocol

import com.google.gson.stream.JsonReader
import kotlinx.coroutines.experimental.Dispatchers
import kotlinx.coroutines.experimental.withContext
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import java.io.IOException
import java.io.StringReader

object GlobalDiscoveryUtil {
    suspend fun queryAnnounceServer(server: String, deviceId: String): AnnouncementMessage {
        return withContext(Dispatchers.IO) {
            val httpGet = HttpGet("https://$server/v2/?device=$deviceId")

            HttpClients.createDefault().execute<AnnouncementMessage>(httpGet) { response ->
                when (response.statusLine.statusCode) {
                    HttpStatus.SC_OK -> {
                        AnnouncementMessage.parse(
                                JsonReader(
                                        StringReader(
                                                EntityUtils.toString(response.entity)
                                        )
                                )
                        )
                    }
                    HttpStatus.SC_NOT_FOUND -> throw DeviceNotFoundException()
                    429 -> throw TooManyRequestsException()
                    else -> throw IOException("http error ${response.statusLine}, response ${EntityUtils.toString(response.entity)}")
                }
            }
        }
    }
}

class DeviceNotFoundException: RuntimeException()
class TooManyRequestsException: RuntimeException()
