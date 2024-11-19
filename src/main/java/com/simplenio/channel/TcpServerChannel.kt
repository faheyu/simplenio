package com.simplenio.channel

import com.simplenio.IOHandler
import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.channels.SelectableChannel
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel

open class TcpServerChannel(private val socketAddress: InetSocketAddress) : IOHandler {

    override val channel: SelectableChannel?
        get() = socketChannel

    protected var socketChannel : ServerSocketChannel? = null

    open fun start() {
        socketChannel?.close()
        socketChannel = ServerSocketChannel.open().apply {
            configureBlocking(false)
            socket().bind(socketAddress, 100)
            NIOManager.register(this@TcpServerChannel, SelectionKey.OP_ACCEPT)
        }
    }

    override fun onRead() {
        NIOManager.register(this@TcpServerChannel, SelectionKey.OP_ACCEPT)
    }

    override fun onWrite() {
        NIOManager.register(this@TcpServerChannel, SelectionKey.OP_ACCEPT)
    }

    override fun onConnected(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun onAccepted() {
        val socket = socketChannel?.accept() ?: return
        val connection = TcpClientChannel(socket)
        NIOManager.register(connection, SelectionKey.OP_READ)
    }
}