package com.simplenio

import java.nio.channels.SelectionKey

object NIOManager {
    /**
     * Stop all selector loops
     */
    fun stopSelector() {
        NIOSelector.stopThread()
    }

    /**
     * register a handler to a selector
     */
    fun register(ioHandler: IOHandler, ops: Int) {
        NIOSelector.register(ioHandler, ops)
    }

    fun registerConnect(ioHandler: IOHandler) {
        register(ioHandler, SelectionKey.OP_CONNECT)
    }

}