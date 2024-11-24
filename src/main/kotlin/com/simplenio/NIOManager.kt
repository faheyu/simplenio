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
     * close and remove channel if [ioHandler] from channel-selector map
     *
     * @param ioHandler handler has channel to close
     */
    fun closeChannel(ioHandler: IOHandler) {
        NIOSelector.closeChannel(ioHandler)
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