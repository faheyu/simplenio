package com.simplenio.channel

import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel

class BaseTCPServerChannel (socketAddress: InetSocketAddress) : AbstractServerChannel(socketAddress) {

    private lateinit var serverChannel : ServerSocketChannel

    override val channel: SelectableChannel
        get() = serverChannel

    override fun start() {
        serverChannel = ServerSocketChannel.open().apply {
            configureBlocking(false)
            socket().bind(socketAddress, 100)
            NIOManager.register(this@BaseTCPServerChannel, SelectionKey.OP_ACCEPT)
        }
    }

    override fun doAccept(): AbstractChannel {
        TODO("Not yet implemented")
    }
}