package com.simplenio.channel

import com.simplenio.DebugLogger
import com.simplenio.IOHandler
import com.simplenio.MyThreadPoolExecutor
import com.simplenio.NIOManager
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder

abstract class AbstractChannel(val socketAddress: InetSocketAddress) : IOHandler {

    companion object {
        /**
         * handle background tasks
         */
        internal val threadPool = MyThreadPoolExecutor(1)

        const val READ_BUFFER_SIZE = 4096
        const val WRITE_BUFFER_SIZE = 4096
        const val RECEIVE_BUFFER_SIZE = 8192

        internal const val CONN_PROC_NOT_CONNECT = 0
        internal const val CONN_PROC_CONNECT = 1
        internal const val CONN_PROC_CONNECTED = 5
        internal const val CONN_PROC_CLOSE = 6

        const val ERR_CONNECT_TIMEOUT = 0
        const val ERR_SERVER_CLOSE = 1
        const val ERR_READ_EXCEPTION = 2
        const val ERR_SEND_EXCEPTION = 3
        const val ERR_CONNECT_EXCEPTION = 4
        const val ERR_CONNECT_ASSERTION = 5
        const val ERR_CONNECTION_NOT_PENDING = 6

        /**
         * Read buffer used to read data from channels.
         *
         * Because we sequentially read through the channels in selector loop,
         * so we can use one buffer for all channels.
         */
        private val sReadBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE)

        /**
         * the object use to lock
         *
         * we use global lock to ensure the channels write sequentially,
         * so we can reuse write buffer to optimize memory.
         */
        private val writeLock = Object()
    }

    private val logger = DebugLogger(javaClass.simpleName)

    /**
     * The buffer used to write data to channel, we can use reusable byte buffer to optimize memory.
     */
    private val mWriteBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE)

    override fun onRead() {
        if (channel == null) {
            logger.e("trying to read null channel $socketAddress")
            return
        }

        do {
            // read until the read buffer is not full
            // to ensure we read all bytes from channel

            // clear first
            sReadBuffer.clear()

            val read = try {
                doRead(sReadBuffer)
            } catch (e: Exception) {
                logger.e("onRead exception $socketAddress", e)

                if (e is IOException) {
                    onError(ERR_READ_EXCEPTION, e.message ?: "")
                }
                return
            }

            if (read < 0) {
                onError(ERR_SERVER_CLOSE, "read $read, server closed connection: $socketAddress")
                return
            } else if (read > 0) {
                sReadBuffer.flip()

                val packet = ByteBuffer.allocate(read)
                packet.order(ByteOrder.LITTLE_ENDIAN)
                packet.put(sReadBuffer)
                packet.flip()
                handlePacket(packet)
            }
        } while (!sReadBuffer.hasRemaining())
    }

    override fun onWrite() {
        if (channel == null) {
            logger.e("trying to write null channel $socketAddress")
            return
        }

        synchronized(writeLock) {
            if (!mWriteBuffer.hasRemaining()) {
                return
            }

            do {
                try {
                    val write = doWrite(mWriteBuffer)
                } catch (e: Exception) {
                    logger.e("onWrite exception, $socketAddress", e)
                    onError(ERR_SEND_EXCEPTION, e.message ?: "")
                }
            } while (mWriteBuffer.hasRemaining())

            mWriteBuffer.clear()
        }
    }

    override fun onAccepted() {
        throw UnsupportedOperationException()
    }

    protected open fun onError(errorCode: Int, info: String?) {
        val reason = when (errorCode) {
            ERR_SERVER_CLOSE -> "server closed"
            ERR_CONNECT_ASSERTION -> "connect assertion"
            ERR_CONNECT_EXCEPTION -> "connect exception"
            ERR_CONNECT_TIMEOUT -> "connect timeout"
            ERR_CONNECTION_NOT_PENDING -> "connection not pending"
            ERR_SEND_EXCEPTION -> "send exception"
            ERR_READ_EXCEPTION -> "read expcetion"
            else -> null
        }
        logger.e(
            "error: $socketAddress, reason=$reason, err=$info\n" +
                    Thread.currentThread().stackTrace.joinToString("\n")
        )
        close()
    }

    /**
     * Perform read operation and returns the number of bytes read.
     *
     * @param readBuffer buffer used to read data from channel, here is [sReadBuffer].
     *
     * @return the number of bytes read
     */
    @Suppress("SameParameterValue")
    protected abstract fun doRead(readBuffer: ByteBuffer) : Int

    /**
     * Perform write operation and returns the number of bytes written.
     *
     * @param writeBuffer buffer used to write to channel, here is [mWriteBuffer].
     *
     * @return the number of bytes written
     */
    protected abstract fun doWrite(writeBuffer: ByteBuffer) : Int

    /**
     * handle received packet
     *
     * @param packet the received packet
     */
    protected abstract fun handlePacket(packet: ByteBuffer)

    /**
     * establish a connection
     */
    abstract fun connect(): Boolean

    /**
     * write data to write buffer, this buffer will be written to channel in selector thread
     *
     * @param data data to send
     */
    @Synchronized
    fun send(data: ByteBuffer) {
        mWriteBuffer.put(data)
        mWriteBuffer.flip()
    }

    /**
     * close the current channel (maybe connected or not)
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