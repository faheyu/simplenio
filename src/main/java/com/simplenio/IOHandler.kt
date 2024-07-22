package com.simplenio

import java.nio.channels.SelectableChannel


/**
 * handle io events
 */
interface IOHandler {
    /**
     * handle read operation
     */
    fun onRead()

    /**
     * handle write operation
     */
    fun onWrite()

    /**
     * callback invoked when channel is connected
     */
    fun onConnected(): Boolean

    /**
     * connected channel
     */
    val channel: SelectableChannel?
}