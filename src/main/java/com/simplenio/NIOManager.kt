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

    fun closeChannel(ioChannel: IOChannel) {
        getSelectorThread(ioChannel).closeChannel(ioChannel)
        // remove from map
        synchronized(channelSelectorMap) {
            channelSelectorMap.remove(ioChannel)
        }
    }

    fun pendingConnect(ioChannel: IOChannel, onConnect: suspend () -> Unit) {
        connectExecutor.launchCoroutine {
            waitForPending(ioChannel)

            withContext(Dispatchers.IO) {
                onConnect()
                register(ioChannel, SelectionKey.OP_CONNECT)
            }
        }
    }

    fun register(ioChannel: IOChannel, ops: Int) {
        getSelectorThread(ioChannel).register(ioChannel, ops)
    }

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

            selectorThread = channelSelectorMap.values.minByOrNull {
                it.channelCount
            } ?: SelectorThread()

            if (selectorThread.channelCount > CHANNEL_PER_SELECTOR) {
                selectorThread = SelectorThread()
            }

            channelSelectorMap[ioChannel] = selectorThread
            logger.d("getSelectorThread channel count: ${selectorThread.channelCount}")
            return selectorThread
        }
    }

    private suspend fun waitForPending(ioChannel: IOChannel) {
        while (!pendingConnections.offer(ioChannel)) {
            logger.d("waiting for pending: ${pendingConnections.size}")
            suspendCoroutine {
                connectContinuations.add(it)
            }
        }
        pendingConnections.remove()
    }

    fun resumeConnect() {
        connectContinuations.poll()?.apply {
            resume(Unit)
        }
    }
}