package com.simplenio.channel

import java.util.concurrent.atomic.AtomicInteger

class ChannelMetrics {

    companion object {
        private val autoIncConnId = AtomicInteger(0)
    }

    var turnOn = false

    @JvmField
    protected var startConnectTime = 0L
    @JvmField
    protected var lastConnectedTime = 0L

    var sentBytes: Long = 0
        protected set
    var receivedBytes: Long = 0
        protected set
    var sentCount = 0
        protected set
    var receivedCount = 0
        protected set
    var lastReceivedTime: Long = 0
        protected set

    @JvmField
    val connId = autoIncConnId.incrementAndGet()
}