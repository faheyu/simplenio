package com.simplenio

import java.nio.channels.SelectableChannel

interface IOChannel {
    fun onRead()
    fun onWrite()
    val channel: SelectableChannel?
    fun onConnected(): Boolean
}