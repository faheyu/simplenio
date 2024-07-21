package com.simplenio

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.SelectionKey
import java.util.concurrent.LinkedBlockingQueue
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

object NIOManager {
    private val logger = DebugLogger("NIORunner")

    private const val MAX_PENDING_CONNECTIONS = 8
    private val pendingConnections = LinkedBlockingQueue<IOChannel>(MAX_PENDING_CONNECTIONS)
    private val connectContinuations = LinkedBlockingQueue<Continuation<Unit>>()
    private val connectExecutor = MyThreadPoolExecutor(MAX_PENDING_CONNECTIONS)

    private const val CHANNEL_PER_SELECTOR = 50
    private val channelSelectorMap = HashMap<IOChannel, SelectorThread>()

    /**
     * Stop all selector threads
     */
    fun stopAllThread() {
        synchronized(channelSelectorMap) {
            channelSelectorMap.values.forEach { it.stopThread() }
        }
    }

    /**
     * close and remove channel from channel-selector map
     *
     * @param ioChannel channel to close
     */
    fun closeChannel(ioChannel: IOChannel) {
        getSelectorThread(ioChannel).closeChannel(ioChannel)
        // remove from map
        synchronized(channelSelectorMap) {
            channelSelectorMap.remove(ioChannel)
        }
    }

    /**
     * request a connection, wait until connection queue is not full
     *
     * @param ioChannel channel perform connection
     * @param onConnect callback invoked when start connection
     */
    fun pendingConnect(ioChannel: IOChannel, onConnect: suspend () -> Unit) {
        connectExecutor.launchCoroutine {
            waitForPending(ioChannel)

            withContext(Dispatchers.IO) {
                onConnect()
                register(ioChannel, SelectionKey.OP_CONNECT)
            }
        }
    }

    /**
     * register a channel to a selector thread
     */
    fun register(ioChannel: IOChannel, ops: Int) {
        getSelectorThread(ioChannel).register(ioChannel, ops)
    }


    /**
     * get selector thread has minimum registered channels
     *
     * @param ioChannel channel need to register
     */
    private fun getSelectorThread(ioChannel: IOChannel) : SelectorThread {
        synchronized(channelSelectorMap) {
            var selectorThread = channelSelectorMap[ioChannel]

            if (selectorThread != null) {
                if (selectorThread.channelCount > CHANNEL_PER_SELECTOR) {
                    // move this channel to other selector thread
                    channelSelectorMap.remove(ioChannel)
                } else {
                    return selectorThread
                }
            }

            selectorThread = channelSelectorMap.values
                .minByOrNull {
                    it.channelCount
                }
                ?: SelectorThread() // create new one if list is empty

            // if this selector thread reach the limit, create new one
            if (selectorThread.channelCount > CHANNEL_PER_SELECTOR) {
                selectorThread = SelectorThread()
            }

            channelSelectorMap[ioChannel] = selectorThread
            logger.d("getSelectorThread channel count: ${selectorThread.channelCount}")
            return selectorThread
        }
    }

    /**
     * wait until connection queue is not full
     *
     * @param ioChannel the channel add to the queue
     */
    private suspend fun waitForPending(ioChannel: IOChannel) {
        // try to add this channel to queue
        while (!pendingConnections.offer(ioChannel)) {
            logger.d("waiting for pending: ${pendingConnections.size}")

            // suspend here until resumed (maybe by selector thread)
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
     * make it continue to connect or queue again
     * if the queue still full
     */
    fun resumeConnect() {
        connectContinuations.poll()?.apply {
            resume(Unit)
            // remove a channel from queue
            // so another connection can be queued
            // the queue should not be empty here
            pendingConnections.remove()
        }
    }
}