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
     * this function invoked when channel is connected
     */
    fun onConnected(): Boolean

    /**
     * this function invoked when channel accepted a new connection
     */
    fun onAccepted()

    /**
     * connected channel
     */
    val channel: SelectableChannel?
}