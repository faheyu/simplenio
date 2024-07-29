package com.simplenio

import kotlinx.coroutines.suspendCancellableCoroutine
import java.nio.channels.SelectionKey
import java.util.concurrent.LinkedBlockingQueue
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume

object NIOManager {
    private val logger = DebugLogger(NIOManager::class.java.simpleName)

    private const val MAX_PENDING_CONNECTIONS = 4
    private val pendingConnections = LinkedBlockingQueue<IOHandler>(MAX_PENDING_CONNECTIONS)
    private val connectContinuations = HashMap<IOHandler, Continuation<Unit>>()
    val threadPool = MyThreadPoolExecutor(MAX_PENDING_CONNECTIONS)

    /**
     * Stop all selector loops
     */
    fun stopSelector() {
        NIOSelector.stopThread()
    }

    /**
     * close and remove channel if [ioHandler] from channel-selector map
     *
     * @param ioHandler handler has channel to close
     */
    fun closeChannel(ioHandler: IOHandler) {
        NIOSelector.closeChannel(ioHandler)
    }

    /**
     * request a connection, wait until connection queue is not full
     *
     * @param ioHandler handler has channel perform connection
     * @param onConnect callback invoked when start connection
     */
    fun pendingConnect(ioHandler: IOHandler, onConnect: suspend () -> Unit) {
        threadPool.launchCoroutine {
            waitForPending(ioHandler)

            // perform connection
            onConnect()
            // after request connection, register it for the selector to handle
            register(ioHandler, SelectionKey.OP_CONNECT)
        }
    }

    /**
     * register a handler to a selector
     */
    fun register(ioHandler: IOHandler, ops: Int) {
        NIOSelector.register(ioHandler, ops)
    }

    /**
     * wait until connection queue is not full, to limit connection request at the same time
     *
     * @param ioHandler handler has channel add to the queue
     */
    private suspend fun waitForPending(ioHandler: IOHandler) {
        // check if this handler already queued
        if (pendingConnections.indexOf(ioHandler) != -1) {
            logger.log("connection already queued, remove it before queue again")
            pendingConnections.remove(ioHandler)
        }

        // try to add this handler to the queue
        // if the queue is full, it will be suspended
        if (!pendingConnections.offer(ioHandler)) {
            logger.log("waiting for pending connection: ${pendingConnections.size}")

            // suspend here until resumed (maybe by selector)
            suspendCancellableCoroutine {
                // add ref to list
                // so we can retrieve and resume it
                // after another connection is established
                synchronized(connectContinuations) {
                    connectContinuations[ioHandler] = it
                }
            }
        }
    }

    /**
     * resume a pending connection (after a connection is established),
     * make it continue to connect or queue again if the queue still full
     */
    fun resumeConnect() {
        // release the queue first
        pendingConnections.poll()

        // resume if any pending connection
        synchronized(connectContinuations) {
            connectContinuations.keys.firstOrNull()?.let {
                connectContinuations.remove(it)?.resume(Unit)
            }
        }
    }
}