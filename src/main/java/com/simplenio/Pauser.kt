package com.simplenio

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume

class Pauser {

    companion object {
        /**
         * default max pause time
         */
        const val MAX_PAUSE_TIME = 30_000L
    }

    var maxPauseTime = MAX_PAUSE_TIME
    private var continuation : Continuation<Unit>? = null

    /**
     * resume at the last paused point, if not paused it does nothing
     */
    fun resume() {
        synchronized(this) {
            continuation?.let {
                // set null before resume as it can be pause immediately after resume
                // leading to pause twice
                continuation = null
                it.resume(Unit)
            }
        }
    }

    /**
     *
     * suspend until resumed
     *
     * @throws IllegalStateException when already paused
     * @throws TimeoutCancellationException when pause too long
     */
    @Throws(IllegalStateException::class, TimeoutCancellationException::class)
    suspend fun pause() {
        withTimeout(maxPauseTime) {
            // use cancellable, so we can cancel it when timeout
            suspendCancellableCoroutine {
                synchronized(this) {
                    if (continuation != null) {
                        throw IllegalStateException("already paused")
                    }

                    continuation = it
                }
            }
        }
    }

}