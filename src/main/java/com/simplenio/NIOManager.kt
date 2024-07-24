package com.simplenio

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.SelectionKey
import java.util.concurrent.LinkedBlockingQueue
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

object NIOManager {
    private val logger = DebugLogger(NIOManager::class.java.simpleName)

    private const val MAX_PENDING_CONNECTIONS = 16
    private val pendingConnections = LinkedBlockingQueue<IOHandler>(MAX_PENDING_CONNECTIONS)
    private val connectContinuations = LinkedBlockingQueue<Continuation<Unit>>()
    private val connectExecutor = MyThreadPoolExecutor(MAX_PENDING_CONNECTIONS)

    private const val HANDLER_PER_SELECTOR = 50
    private val channelSelectorMap = HashMap<IOHandler, NIOSelector>()

    /**
     * Stop all selector loops
     */
    fun stopAllSelectors() {
        synchronized(channelSelectorMap) {
            channelSelectorMap.values.forEach { it.stop() }
        }
    }

    /**
     * close and remove channel if [ioHandler] from channel-selector map
     *
     * @param ioHandler handler has channel to close
     */
    fun closeChannel(ioHandler: IOHandler) {
        getSelector(ioHandler).closeChannel(ioHandler)
        // remove from map
        synchronized(channelSelectorMap) {
            channelSelectorMap.remove(ioHandler)
        }
    }

    /**
     * request a connection, wait until connection queue is not full
     *
     * @param ioHandler handler has channel perform connection
     * @param onConnect callback invoked when start connection
     */
    fun pendingConnect(ioHandler: IOHandler, onConnect: suspend () -> Unit) {
        connectExecutor.launchCoroutine {
            waitForPending(ioHandler)

            withContext(Dispatchers.IO) {
                // perform connection
                onConnect()
                // after request connection, register it for the selector to handle
                register(ioHandler, SelectionKey.OP_CONNECT)
            }
        }
    }

    /**
     * register a handler to a selector
     */
    fun register(ioHandler: IOHandler, ops: Int) {
        getSelector(ioHandler).register(ioHandler, ops)
    }


    /**
     * get selector has minimum registered handlers
     *
     * @param ioHandler handler needs to be registered
     */
    private fun getSelector(ioHandler: IOHandler) : NIOSelector {
        synchronized(channelSelectorMap) {
            var selector = channelSelectorMap[ioHandler]

            if (selector != null) {
                if (selector.handlerCount > HANDLER_PER_SELECTOR) {
                    // remove to move this handler to other selector
                    channelSelectorMap.remove(ioHandler)
                } else {
                    return selector
                }
            }

            selector = channelSelectorMap.values
                .minByOrNull {
                    it.handlerCount
                }
                ?: NIOSelector() // create new one if list is empty

            // if this selector reach the limit, create new one
            if (selector.handlerCount > HANDLER_PER_SELECTOR) {
                selector = NIOSelector()
            }

            channelSelectorMap[ioHandler] = selector
            logger.d("getSelector handler count: ${selector.handlerCount}")
            return selector
        }
    }

    /**
     * wait until connection queue is not full, to limit connection request at the same time
     *
     * @param ioHandler handler has channel add to the queue
     */
    private suspend fun waitForPending(ioHandler: IOHandler) {
        // try to add this handler to the queue
        while (!pendingConnections.offer(ioHandler)) {
            logger.d("waiting for pending: ${pendingConnections.size}")

            // suspend here until resumed (maybe by selector)
            suspendCoroutine {
                // add ref to list
                // so we can retrieve and resume it
                // after another connection is established
                connectContinuations.add(it)
            }
        }
    }

    /**
     * resume a pending connection (after a connection is established),
     * make it continue to connect or queue again if the queue still full
     */
    fun resumeConnect() {
        connectContinuations.poll()?.apply {
            // remove a handler from queue
            // so another connection can be queued
            // the queue should not be empty here
            pendingConnections.remove()

            // resume after remove from queue
            // to prevent queuing again
            resume(Unit)
        }
    }
}