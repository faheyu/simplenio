package com.simplenio.channel

import com.simplenio.IOHandler
import com.simplenio.MyThreadPoolExecutor
import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.ByteBuffer

abstract class AbstractChannel(val socketAddress: InetSocketAddress) : IOHandler {

    companion object {
        /**
         * handle background tasks
         */
        internal val threadPool = MyThreadPoolExecutor(1)

        const val READ_BUFFER_SIZE = 8192
        const val WRITE_BUFFER_SIZE = 8192
    }

    /**
     * data buffer read from channel
     */
    protected val readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE)

    /**
     * data buffer write to channel
     */
    protected val writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE)

    /**
     * establish a connection
     */
    abstract fun connect(): Boolean

    /**
     * write data to buffer, this buffer will be write to channel in selector thread
     *
     * @return true if buffer's capacity > 0 else false
     */
    abstract fun send(byteBuffer: ByteBuffer): Boolean

    /**
     * close current channel (maybe connected or not)
     */
    open fun close() {
        NIOManager.closeChannel(this)
    }

    protected fun finalize() {
        // somehow we forget to close channel
        // this is "memory guard"
        close()
    }
}