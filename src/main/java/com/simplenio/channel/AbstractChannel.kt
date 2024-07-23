package com.simplenio.channel

import com.simplenio.IOHandler
import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.ByteBuffer

abstract class AbstractChannel(val socketAddress: InetSocketAddress) : IOHandler {

    companion object {
        const val READ_BUFFER_SIZE = 8192
        const val WRITE_BUFFER_SIZE = 8192
    }

    /**
     * data buffer read from socket
     */
    protected val readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE)

    /**
     * data buffer write to socket
     */
    protected val writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE)

    /**
     * establish a connection
     */
    abstract fun connect(): Boolean

    /**
     * write data to socket
     */
    abstract fun send(byteBuffer: ByteBuffer): Boolean

    /**
     * close current socket (maybe connected or not)
     */
    open fun close() {
        NIOManager.closeChannel(this)
    }

    protected fun finalize() {
        // somehow we forget to close socket
        // this is "memory guard"
        close()
    }
}