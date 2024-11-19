package com.simplenio.channel

import com.simplenio.NIOManager
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectableChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.Future
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

open class TcpClientChannel : AbstractChannel {
    companion object {
        const val SO_TIMEOUT = 5_000
        const val CONNECT_TIMEOUT = 10_000L

        private val logger = Logger.getLogger("TcpClientChannel")
        private val threadPool = ScheduledThreadPoolExecutor(1)

        /**
         * optimize channel config to work with non-blocking mode
         */
        fun setupChannel(channel: SocketChannel) {
            channel.apply {
                configureBlocking(false)
                socket().soTimeout = SO_TIMEOUT
                socket().tcpNoDelay = true
            }
        }
    }

    var keepAliveTimeout = 10_000L

    override val channel: SelectableChannel?
        get() = socketChannel

    private var socketChannel : SocketChannel? = null
    private var mConnectTimeoutCheckTask : Future<*>? = null
    private var mKeepAliveTimeoutCheckTask : Future<*>? = null

    private val writeLock = Object()

    /**
     * The buffer used to send data partly
     */
    private var mWriteBuffer : ByteBuffer? = null
    private lateinit var mReceiveBuffer : ByteBuffer

    private var lastActiveTime = 0L

    constructor(socketAddress: InetSocketAddress): super(socketAddress)
    constructor(connectedSocket: SocketChannel) : super(connectedSocket.remoteAddress as InetSocketAddress) {
        if (!connectedSocket.isConnected)
            throw IllegalArgumentException("socket is not connected")

        socketChannel = connectedSocket
        setupChannel(socketChannel!!)
        connProc = CONN_PROC_CONNECTED
    }

    fun connect(): Boolean {
        logger.info("Connecting to: $socketAddress")

        return try {
            socketChannel = SocketChannel.open().apply {
                setupChannel(this)
                connect(socketAddress)
                connProc = CONN_PROC_CONNECTING
                waitConnectTimeout()
            }
            NIOManager.registerConnect(this)

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

    fun send(buffer: ByteBuffer) : Boolean {
        return doWrite(buffer) > 0
    }

    protected open fun validatePacket(receiveBuffer: ByteBuffer) : Boolean {
        return true
    }

    protected open fun readPacket(receiveBuffer: ByteBuffer): Boolean {
        receiveBuffer.clear()
        return true
    }

    override fun close() {
        super.close()
        removeWaitConnectTimeout()
        socketChannel = null
    }

    override fun onRead() {
        if (channel == null) {
            logger.warning("trying to read null channel $socketAddress")
            return
        }

        do {
            // clear first
            sReadBuffer.clear()

            val read = try {
                socketChannel!!.read(sReadBuffer)
            } catch (e: Exception) {
                logger.severe("onRead exception $socketAddress\n${e.stackTraceToString()}")

                if (e is IOException) {
                    onError(ERR_READ_EXCEPTION, e.message ?: "")
                }
                return
            }

            if (read < 0) {
                onError(ERR_PEER_CLOSE, "read $read, peer closed connection: $socketAddress")
                return
            } else if (read > 0) {
                sReadBuffer.flip()

                // copy to new buffer to read other packets
                val packet = ByteBuffer.allocate(read)
                packet.put(sReadBuffer)
                packet.flip()
                receivePacket(packet)
            }

            // read until the read buffer is not full
            // to ensure we read all bytes from channel
        } while (read >= sReadBuffer.capacity())
    }

    override fun onWrite() {
        mWriteBuffer?.also {
            doWrite(null)
        }
    }

    override fun onConnected(): Boolean {
        return try {
            if (!socketChannel!!.isConnectionPending) {
                removeWaitConnectTimeout()
                onError(ERR_CONNECTION_NOT_PENDING, "not in connection pending state.")
                return false
            }

            if (!socketChannel!!.finishConnect()) {
                logger.info("still connecting...$socketAddress")
                return false
            }

            // channel is connected here
            logger.info("Connected to: $socketAddress")
            connProc = CONN_PROC_CONNECTED
            removeWaitConnectTimeout()

            // auto close channel when inactive
            lastActiveTime = System.currentTimeMillis()
            mKeepAliveTimeoutCheckTask?.cancel(true)
            mKeepAliveTimeoutCheckTask = threadPool.scheduleWithFixedDelay(
                {
                    if(lastActiveTime + keepAliveTimeout <= System.currentTimeMillis()) {
                        logger.warning("keep alive timeout, close the channel")
                        close()
                        mKeepAliveTimeoutCheckTask?.cancel(false)
                        mKeepAliveTimeoutCheckTask = null
                    }
                },
                1_000L,
                1_000L,
                TimeUnit.MILLISECONDS
            )

            return true
        } catch (e: Exception) {
            logger.severe("onConnected exception\n${e.stackTraceToString()}")
            removeWaitConnectTimeout()
            onError(ERR_CONNECT_EXCEPTION, e.message)
            false
        }
    }

    override fun onAccepted() {
        throw UnsupportedOperationException()
    }

    private fun receivePacket(packet: ByteBuffer) {
        when (connProc) {
            CONN_PROC_CONNECTED -> {
                if (!this::mReceiveBuffer.isInitialized) {
                    mReceiveBuffer = ByteBuffer.allocate(RECEIVE_BUFFER_SIZE)
                }

                // put new buffer
                // expand if needed
                mReceiveBuffer = mReceiveBuffer.let {
                    if (it.remaining() < packet.limit()) {
                        /**
                         * current position + remaining = limit
                         * but remaining < byteBuffer.limit()
                         * so new limit is current position + byteBuffer's limit
                         */
                        val newLimit = it.position() + packet.limit()

                        /**
                         * ensure new capacity is not less than multiple of size
                         */
                        val newCapacity = (newLimit / READ_BUFFER_SIZE + 1) * READ_BUFFER_SIZE
                        val newBuffer = ByteBuffer.allocate(newCapacity)

                        // transfer data
                        it.flip()
                        newBuffer.put(it)
                        newBuffer.put(packet)
                        return@let newBuffer
                    }

                    // no need to expand, just put it normally
                    it.put(packet)
                    packet.clear()
                    return@let it
                }

                // read packet
                mReceiveBuffer.apply {
                    if (position() > 0 && validatePacket(this)) {
                        do {
                            val readPacket = readPacket(this)
                            if (readPacket) {
                                lastActiveTime = System.currentTimeMillis()
                            }
                        } while (readPacket)
                    }
                }
            }
            else -> logger.warning("receivePacket, invalid conn: $connProc")
        }
    }

    private fun doWrite(outBuffer: ByteBuffer?): Int {
        val channel = socketChannel

        if (channel == null || !channel.isConnected) {
            logger.warning("trying to write null or not connected channel $socketAddress")
            return -1
        }

        synchronized(writeLock) {
            if (outBuffer == null && mWriteBuffer == null) {
                return -2
            }

            return try {
                //TODO: implement write data partly
                channel.write(outBuffer)
            } catch (e: Exception) {
                logger.warning("doWrite exception, $socketAddress\n${e.stackTraceToString()}")
                onError(ERR_SEND_EXCEPTION, e.message ?: "")
                -1
            }
        }

    }

    private fun waitConnectTimeout() {
        mConnectTimeoutCheckTask?.cancel(true)
        mConnectTimeoutCheckTask = threadPool.schedule(
            {
                if (connProc < CONN_PROC_CONNECTED && socketChannel?.isConnected != true) {
                    onError(ERR_CONNECT_TIMEOUT, "connecting timeout $socketAddress")
                }
            },
            CONNECT_TIMEOUT,
            TimeUnit.MILLISECONDS
        )
    }

    private fun removeWaitConnectTimeout() {
        mConnectTimeoutCheckTask?.cancel(true)
        mConnectTimeoutCheckTask = null
    }
}