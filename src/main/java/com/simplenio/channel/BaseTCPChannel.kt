package com.simplenio.channel

import com.simplenio.DebugLogger
import com.simplenio.NIOManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectableChannel
import java.nio.channels.SocketChannel


open class BaseTCPChannel(socketAddress: InetSocketAddress) : AbstractChannel(socketAddress) {

    companion object {
        private val logger = DebugLogger(BaseTCPChannel::class.java.simpleName)
        private const val SO_TIMEOUT = 5_000L
        private const val CONNECT_TIMEOUT = 10_000L
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
        threadPool.removeSavedTask(mTimeoutCheckTask)
        threadPool.scheduleAndSave(mTimeoutCheckTask, CONNECT_TIMEOUT)
    }

    private fun removeWaitConnectTimeout() {
        threadPool.removeSavedTask(mTimeoutCheckTask)
    }

    override fun doRead(readBuffer: ByteBuffer): Int {
        return socketChannel!!.read(readBuffer)
    }

    override fun doWrite(writeBuffer: ByteBuffer): Int {
        val channel = socketChannel
        if (channel == null || !channel.isConnected) {
            logger.e("trying to write null or not connected channel $socketAddress")
            return -1
        }

        return channel.write(writeBuffer)
    }

    override fun handlePacket(packet: ByteBuffer) {
        TODO("Not yet implemented")
    }

    override val channel: SelectableChannel?
        get() = socketChannel

    override fun onConnected(): Boolean {
        return try {
            if (!socketChannel!!.isConnectionPending) {
                removeWaitConnectTimeout()
                onError(ERR_CONNECTION_NOT_PENDING, "not in connection pending state.")
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

    override fun close() {
        super.close()
        socketChannel = null
        connProc = CONN_PROC_CLOSE
    }
}