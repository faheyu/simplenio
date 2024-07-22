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
        const val MIN_SELECT_TIME = 100L
    }

    private class Registration(val ops: Int, val ioHandler: IOHandler)

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
                // if selector has not opened yet, return registration count instead
                if (!this::mSelector.isInitialized) {
                    return synchronized(pendingRegistrations) {
                        pendingRegistrations.map { it.ioHandler }.toSet().size
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
                        registration.ioHandler.channel?.register(
                            mSelector,
                            registration.ops,
                            registration.ioHandler
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
        val ioHandler = key.attachment() as IOHandler

        try {
            if (ioHandler.channel == null) {
                key.cancel()
            } else {
                var interestOps = key.interestOps()

                if (key.isConnectable) {
                    if (ioHandler.onConnected()) {
                        interestOps = SelectionKey.OP_READ or SelectionKey.OP_WRITE
                    }
                } else if (key.isValid) {
                    if (key.isReadable) {
                        ioHandler.onRead()
                    }
                    if (key.isWritable) {
                        ioHandler.onWrite()
                    }
                }

                if (key.interestOps() != interestOps)
                    register(ioHandler, interestOps)
            }
        } catch (_: CancelledKeyException) {

        }
    }

    fun register(ioHandler: IOHandler, ops: Int) {
        start()

        // add to the queue, selector thread will register it later
        synchronized(pendingRegistrations) {
            pendingRegistrations.addLast(Registration(ops, ioHandler))
        }

        // we don't need to wake up selector to prevent blocking
        // as we had queued this registration which will be registered in selector thread
    }

    fun stopThread() {
        selectorLock.lock()
        try {
            if (this::mSelector.isInitialized) {
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

    fun closeChannel(ioHandler: IOHandler) {
        synchronized(ioHandler) {
            try {
                ioHandler.channel?.close()
            } catch (e: Throwable) {
                logger.e("closeChannel error", e)
            }
        }
    }
}