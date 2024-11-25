package com.simplenio

import java.io.IOException
import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger
import kotlin.system.measureTimeMillis

internal object NIOSelector : Thread() {

    private val logger = Logger.getLogger("NIOSelector")

    /**
     * ensure select loop time is not less than this value, to reduce cpu load
     */
    private const val MIN_SELECT_LOOP_TIME = 100L

    /**
     * when select loop time greater than this value,
     * it will throw a [RuntimeException] to ensure select loop should not be blocking
     */
    private const val MAX_SELECT_LOOP_TIME = 200L

    private class Registration(val ops: Int, val ioHandler: IOHandler)

    /**
     * lock when invoke any selector operation
     */
    private val selectorLock = ReentrantLock()

    @Volatile
    private lateinit var mSelector: Selector

    /**
     * pending registrations to register in select loop
     */
    private val pendingRegistrations = LinkedList<Registration>()

    /**
     * determine whether it is running or not
     */
    @Volatile
    private var running = false

    /**
     * determine whether it has started or not
     */
    private val started = AtomicBoolean(false)

    /**
     * for measuring select loop time.
     *
     * Use property so we don't have to create a new object every loop
     */
    private var loopTime = 0L

    override fun start() {
        if (started.getAndSet(true))
            return

        super.start()
        logger.info("selector started")
    }

    override fun run() {
        running = true

        // open selector first
        selectorLock.lock()
        try {
            mSelector = Selector.open()
        } finally {
            selectorLock.unlock()
        }

        while (running) {
            try {
                // register all pending
                synchronized(pendingRegistrations) {
                    pendingRegistrations.forEach { registration ->
                        registration.ioHandler.channel?.register(
                            mSelector,
                            registration.ops,
                            registration.ioHandler
                        )
                    }

                    // clear for new registrations
                    pendingRegistrations.clear()
                }

                // wait for any selector operations is completed
                // see https://stackoverflow.com/questions/1057224/java-thread-blocks-while-registering-channel-with-selector-while-select-is-cal/2179612#2179612
                // we can lock and unlock here
                // as it's in same thread
                selectorLock.lock()
                selectorLock.unlock()

                val keyCount = mSelector.select(MIN_SELECT_LOOP_TIME)
                if (keyCount > 0) {
                    val it = mSelector.selectedKeys().iterator()
                    loopTime = measureTimeMillis {
                        while (it.hasNext()) {
                            val selectionKey = it.next()
                            it.remove()

                            if (selectionKey.isValid) {
                                handleKey(selectionKey)
                            }
                        }
                    }
                }

                if (loopTime > MAX_SELECT_LOOP_TIME) {
                    /**
                     * This ensure select loop should not be blocking
                     */
                    throw RuntimeException("Select loop too long: $loopTime ms")
                } else {
                    sleep((MIN_SELECT_LOOP_TIME - loopTime).coerceAtLeast(0))
                }

            } catch (_: CancelledKeyException) {

            } catch (e: Throwable) {
                logger.severe("NIO selector exception: $e\n${e.stackTraceToString()}")
            }
        }
    }

    /**
     * handle IO operations
     *
     * @param key selected key
     */
    private fun handleKey(key: SelectionKey) {
        val ioHandler = key.attachment() as IOHandler

        try {
            if (ioHandler.channel == null) {
                // if channel is closed or not connected, cancel the key
                key.cancel()
                logger.info("cancel key due to channel is null")
            } else {
                if (key.isAcceptable) {
                    ioHandler.onAccepted()
                } else if (key.isConnectable && ioHandler.onConnected()) {
                    // ready for i/o operations
                    register(ioHandler, SelectionKey.OP_READ or SelectionKey.OP_WRITE)
                } else if (key.isValid) {
                    if (key.isReadable) {
                        ioHandler.onRead()
                    } else if (key.isWritable) {
                        ioHandler.onWrite()
                    }
                }
            }
        } catch (_: CancelledKeyException) {

        }
    }

    /**
     * queue registration of this [ioHandler] to the queue
     *
     * @param ioHandler handler to register
     * @param ops selection key ops
     */
    fun register(ioHandler: IOHandler, ops: Int) {
        start()

        // add to the queue, selector thread will register it later
        synchronized(pendingRegistrations) {
            pendingRegistrations.addLast(Registration(ops, ioHandler))
        }

        // we don't need to wake up selector to prevent blocking
        // as we had queued this registration which will be registered in selector thread
    }

    /**
     * Stop and close select loop
     */
    fun stopThread() {
        // if it has been stopped or has not started, do nothing
        if (!started.getAndSet(false))
            return

        running = false

        // clear the registration queue
        synchronized(pendingRegistrations) {
            pendingRegistrations.iterator().let { listItertator ->
                // loop through registration queue
                while (listItertator.hasNext()) {
                    listItertator.next().let { registration ->
                        // remove and close channel
                        registration.ioHandler.channel?.close()
                        listItertator.remove()
                    }
                }
            }
        }

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

        logger.info("waiting for thread stopped")
        join()
    }

}