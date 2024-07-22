package com.simplenio

import java.io.IOException
import java.lang.Thread.UncaughtExceptionHandler
import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

class SelectorThread : Thread() {

    companion object {
        private val logger = DebugLogger("SelectorThread")

        /**
         * prevent select loop too fast
         */
        const val MIN_SELECT_TIME = 500L
    }

    private class Registration(val ops: Int, val ioChannel: IOChannel)

    private val selectorLock = ReentrantLock()

    @Volatile
    private lateinit var mSelector: Selector

    private val pendingRegistrations = LinkedList<Registration>()

    @Volatile
    private var running = false
    private val started = AtomicBoolean(false)

    init {
        uncaughtExceptionHandler = UncaughtExceptionHandler { _, e -> throw e }
    }

    val channelCount : Int
        get() {
            selectorLock.lock()
            try {
                if (!this::mSelector.isInitialized) {
                    return synchronized(pendingRegistrations) {
                        pendingRegistrations.map { it.ioChannel }.toSet().size
                    }
                }

                return mSelector.keys().size
            } finally {
                selectorLock.unlock()
            }
        }

    override fun start() {
        if (started.getAndSet(true)) return
        super.start()
    }

    // https://stackoverflow.com/questions/1057224/java-thread-blocks-while-registering-channel-with-selector-while-select-is-cal/2179612#2179612
    override fun run() {
        running = true

        selectorLock.lock()
        try {
            mSelector = Selector.open()
        } finally {
            selectorLock.unlock()
        }

        while (running) {
            selectorLock.lock()
            selectorLock.unlock()

            try {

                synchronized(pendingRegistrations) {
                    pendingRegistrations.forEach { registration ->
                        registration.ioChannel.channel?.register(
                            mSelector,
                            registration.ops,
                            registration.ioChannel
                        )
                    }
                    pendingRegistrations.clear()
                }

                sleep(MIN_SELECT_TIME)
                mSelector.selectNow()
                val it = mSelector.selectedKeys().iterator()
                val startMills = System.currentTimeMillis()

                while (it.hasNext()) {
                    val selectionKey = it.next()
                    it.remove()

                    val isConnectOp = selectionKey.interestOps() and SelectionKey.OP_CONNECT == SelectionKey.OP_CONNECT

                    if (selectionKey.isValid) {
                        handleKey(selectionKey)
                    }

                    if (isConnectOp) {
                        NIOManager.resumeConnect()
                    }
                }

                val loopTime = System.currentTimeMillis() - startMills
                if (loopTime > 500)
                    logger.w("select loop took $loopTime ms")

            } catch (_: CancelledKeyException) {

            } catch (e: Throwable) {
                logger.e("NIO selector thread exception", e)
            }
        }
    }

    private fun handleKey(key: SelectionKey) {
        val ioChannel = key.attachment() as IOChannel

        try {
            if (ioChannel.channel == null) {
                key.cancel()
            } else {
                var interestOps = key.interestOps()

                if (key.isConnectable) {
                    if (ioChannel.onConnected()) {
                        interestOps = SelectionKey.OP_READ or SelectionKey.OP_WRITE
                    }
                } else if (key.isValid) {
                    if (key.isReadable) {
                        ioChannel.onRead()
                    }
                    if (key.isWritable) {
                        ioChannel.onWrite()
                    }
                }

                if (key.interestOps() != interestOps)
                    register(ioChannel, interestOps)
            }
        } catch (_: CancelledKeyException) {

        }
    }

    fun register(ioChannel: IOChannel, ops: Int) {
        start()

        synchronized(pendingRegistrations) {
            pendingRegistrations.addLast(Registration(ops, ioChannel))
        }

        selectorLock.lock()
        if (this::mSelector.isInitialized) {
            mSelector.wakeup()
        }
        selectorLock.unlock()
    }

    fun stopThread() {
        selectorLock.lock()
        try {
            if (this::mSelector.isInitialized) {
                mSelector.wakeup()
                try {
                    mSelector.close()
                } catch (_: IOException) {

                }
            }
        } finally {
            selectorLock.unlock()
        }

        running = false
    }

    fun closeChannel(ioChannel: IOChannel) {
        synchronized(ioChannel) {
            selectorLock.lock()
            try {
                if (this::mSelector.isInitialized) {
                    mSelector.wakeup()
                }

                ioChannel.channel?.close()
            } catch (e: Throwable) {
                logger.e("closeChannel error", e)
            } finally {
                selectorLock.unlock()
            }
        }
    }
}