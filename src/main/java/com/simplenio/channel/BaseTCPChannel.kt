package com.simplenio.channel

import com.simplenio.DebugLogger
import com.simplenio.NIOManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectableChannel
import java.nio.channels.SocketChannel


open class BaseTCPChannel(socketAddress: InetSocketAddress) : AbstractChannel(socketAddress) {

    companion object {
        private val logger = DebugLogger(BaseTCPChannel::class.java.simpleName)
        private const val SO_TIMEOUT = 5_000L
        private const val CONNECT_TIMEOUT = 10_000L

        private const val CONN_PROC_NOT_CONNECT = 0
        private const val CONN_PROC_CONNECT = 1
        private const val CONN_PROC_CONNECTED = 5
        private const val CONN_PROC_CLOSE = 6

        const val ERR_CONNECT_TIMEOUT = 0
        const val ERR_SERVER_CLOSE = 1
        const val ERR_READ_EXCEPTION = 2
        const val ERR_SEND_EXCEPTION = 3
        const val ERR_CONNECT_EXCEPTION = 4
        const val ERR_CONNECT_ASSERTION = 5
        const val ERR_CONNECTION_NOT_PENDING = 6
    }

    /**
     * connect process
     */
    private var connProc = CONN_PROC_NOT_CONNECT

    private val mTimeoutCheckTask = Runnable {
        if (connProc < CONN_PROC_CONNECTED && socketChannel?.isConnected != true) {
            onError(ERR_CONNECT_TIMEOUT, "connecting timeout $socketAddress")
        }
    }

    private var socketChannel : SocketChannel? = null

    private fun waitConnectTimeout() {
        threadPool.remove(mTimeoutCheckTask)
        threadPool.schedule(mTimeoutCheckTask, CONNECT_TIMEOUT)
    }

    private fun removeWaitConnectTimeout() {
        threadPool.remove(mTimeoutCheckTask)
    }

    private fun doSend(): Int {
        @Suppress("BlockingMethodInNonBlockingContext")
        return if (!writeBuffer.hasRemaining()) {
            -1
        } else try {
            val socketChannel = socketChannel

            if (socketChannel != null && socketChannel.isConnected) {
                synchronized(this) {
                    // perform write
                    return this.socketChannel!!.write(writeBuffer)
                }
            }

            logger.e("trying to write null or not connected channel $socketAddress")
            -1
        } catch (e: Exception) {
            logger.e("doSend exception, $socketAddress", e)
            onError(ERR_SEND_EXCEPTION, e.message)
            -1
        }
    }

    override suspend fun onRead() {
        if (socketChannel == null) {
            logger.e("trying to read null channel $socketChannel")
            return
        }

        try {
            readBuffer.clear()
            val read = socketChannel!!.read(readBuffer)

            if (read <= 0) {
                onError(ERR_SERVER_CLOSE, "read $read, server closed, conn: $socketChannel")
            }
        } catch (e: Exception) {
            logger.e("TCP onRead exception @$socketAddress", e)
            if (e is IOException) {
                onError(ERR_READ_EXCEPTION, e.message)
            }
        }
    }

    override suspend fun onWrite() {
        logger.d("onWrite send buffer, len:" + writeBuffer.capacity())
        doSend()
    }

    override val channel: SelectableChannel?
        get() = socketChannel

    open fun onError(code: Int, info: String?) {
        val reason = when (code) {
            ERR_SERVER_CLOSE -> "server closed"
            ERR_CONNECT_ASSERTION -> "connect assertion"
            ERR_CONNECT_EXCEPTION -> "connect exception"
            ERR_CONNECT_TIMEOUT -> "connect timeout"
            ERR_CONNECTION_NOT_PENDING -> "connection not pending"
            ERR_SEND_EXCEPTION -> "send exception"
            ERR_READ_EXCEPTION -> "read expcetion"
            else -> null
        }

        logger.e("error: $socketAddress, reason=$reason, err=$info", Exception())
        close()
    }

    override suspend fun onConnected(): Boolean {
        return try {
            if (!socketChannel!!.isConnectionPending) {
                logger.e("TCP is not in connection pending state.")
                removeWaitConnectTimeout()
                onError(ERR_CONNECTION_NOT_PENDING, null)
                return false
            }

            if (!socketChannel!!.finishConnect()) {
                logger.e("still connecting...$socketAddress")
                return false
            }

            logger.d("Connected to: $socketAddress")
            removeWaitConnectTimeout()

            return true
        } catch (e: Exception) {
            logger.e("onConnected exception", e)
            removeWaitConnectTimeout()
            onError(ERR_CONNECT_EXCEPTION, e.message)
            false
        }
    }

    override fun connect(): Boolean {
        logger.d("Connecting to: $socketAddress")

        return try {
            NIOManager.pendingConnect(this) {
                socketChannel = withContext(Dispatchers.IO) {
                    SocketChannel.open().apply {
                        configureBlocking(false)
                        socket().soTimeout = SO_TIMEOUT.toInt()
                        socket().tcpNoDelay = true
                        connect(socketAddress)
                        connProc = CONN_PROC_CONNECT
                        waitConnectTimeout()
                    }
                }
            }

            readBuffer.clear()
            true
        } catch (e: Throwable) {
            removeWaitConnectTimeout()

            if (e is AssertionError) {
                onError(ERR_CONNECT_ASSERTION, e.message)
            } else {
                onError(ERR_CONNECT_EXCEPTION, e.message)
            }

            false
        }
    }

    override fun send(byteBuffer: ByteBuffer): Boolean {
        writeBuffer.clear()
        writeBuffer.put(byteBuffer)
        writeBuffer.flip()
        return writeBuffer.capacity() > 0
    }

    override fun close() {
        super.close()
        socketChannel = null
        connProc = CONN_PROC_CLOSE
    }
}