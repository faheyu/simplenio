package com.simplenio

import java.nio.channels.SelectableChannel


/**
 * handle io events
 */
interface IOHandler {
    /**
     * handle read operation
     */
    suspend fun onRead()

    /**
     * handle write operation
     */
    suspend fun onWrite()

    /**
     * callback invoked when channel is connected
     */
    suspend fun onConnected(): Boolean

    /**
     * connected channel
     */
    val channel: SelectableChannel?
}