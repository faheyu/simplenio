package com.simplenio.channel

import com.simplenio.IOHandler
import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.channels.SelectionKey

abstract class AbstractServerChannel (val socketAddress: InetSocketAddress) : IOHandler {

    /**
     * start and ready to accept client connections
     */
    abstract fun start()

    /**
     * accept a client socket channel and return a client channel handler.
     */
    abstract fun doAccept() : AbstractChannel

    override fun onAccepted() {
        val client = doAccept()
        NIOManager.register(client, SelectionKey.OP_READ or SelectionKey.OP_WRITE)
    }

    override fun onConnected(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun onRead() {
        throw UnsupportedOperationException()
    }

    override fun onWrite() {
        throw UnsupportedOperationException()
    }
}