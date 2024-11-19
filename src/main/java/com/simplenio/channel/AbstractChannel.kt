package com.simplenio.channel

import com.simplenio.IOHandler
import com.simplenio.NIOManager
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.logging.Logger

abstract class AbstractChannel(val socketAddress: InetSocketAddress) : IOHandler {

    companion object {
        private val logger = Logger.getLogger("AbstractChannel")

        const val READ_BUFFER_SIZE = 4096
        const val WRITE_BUFFER_SIZE = 4096
        const val RECEIVE_BUFFER_SIZE = 32768

        internal const val CONN_PROC_NOT_CONNECT = 0
        internal const val CONN_PROC_CONNECTING = 1
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
        internal val sReadBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE)
    }

    protected var connProc = CONN_PROC_NOT_CONNECT

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
        logger.severe(
            "error: $socketAddress, reason=$reason, err=$info\n" +
                    Thread.currentThread().stackTrace.joinToString("\n")
        )
        close()
    }

    /**
     * close the current channel (maybe connected or not)
     */
    open fun close() {
        logger.info("close channel $socketAddress")
        NIOManager.closeChannel(this)
        connProc = CONN_PROC_CLOSE
    }
}