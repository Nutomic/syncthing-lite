package net.syncthing.java.core.utils

import java.io.IOException

object NetworkUtils {

    @Throws(IOException::class)
    fun assertProtocol(value: Boolean, lazyMessage: (() -> String)? = null) {
        if (!value) {
            if (lazyMessage != null)
                throw IOException(lazyMessage())
            else
                throw IOException()
        }
    }
}