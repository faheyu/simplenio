package com.simplenio.tuning

import com.simplenio.DebugLogger
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

object ByteBufferPool : ObjectPool<ByteBuffer>() {

    private val logger = DebugLogger(ByteBufferPool::class.java.simpleName)

    private const val MAX_BUFFER_SIZE = 512
    private const val MAX_BUFFER_ALLOCATED = 64 * 1024 * 1024 // 64MB
    private const val SLEEP_TIME = 100L
    private val allocInUse = AtomicInteger()

    /**
     * Get and clear byte buffer not in used in the pool or allocate new one if not found any.
     * The returned byte buffer will have capacity >= given [capacity] and limit = given [capacity].
     * So when we want to get the size of data in buffer to read or write, use [ByteBuffer.limit] instead of [ByteBuffer.capacity].
     *
     * @param capacity byte buffer's capacity
     * @param isDirect allocate direct or not
     *
     * @return the [ObjectPool.ReusableObject] holds the byte buffer reference
     */
    @JvmStatic
    fun getByteBuffer(capacity: Int, isDirect: Boolean = false) : ReusableObject {
        while (allocInUse.get() > MAX_BUFFER_ALLOCATED) {
            Thread.sleep(SLEEP_TIME)
            logger.log("waiting for buffer reuse, total allocated $allocInUse")
        }

        // get byte buffer has smallest capacity
        var reusableByteBuffer = getMinByOrNull {
            // set compare value out of the sort
            // in order to ignore these byte buffer
            if (it.isDirect != isDirect)
                return@getMinByOrNull Int.MAX_VALUE
            if (it.capacity() < capacity)
                return@getMinByOrNull Int.MAX_VALUE

            it.capacity()
        }?.let {
            val byteBuffer = it.get()

            // we can get the byte buffer has capacity < capacity to use
            // which means no such byte buffer has capacity equals or greater than capacity need to use
            if (byteBuffer.capacity() < capacity)
                return@let null

            // check if byte buffer is direct or not
            if (byteBuffer.isDirect != isDirect)
                return@let null

            it
        }

        // allocate new one if not found
        if (reusableByteBuffer == null) {
            reusableByteBuffer = if (isDirect) {
                add(ByteBuffer.allocateDirect(capacity), true)
            } else {
                add(ByteBuffer.allocate(capacity), true)
            }
        }

        val byteBuffer = reusableByteBuffer.get()
        byteBuffer.clear().limit(capacity)
        return reusableByteBuffer.also {
            allocInUse.addAndGet(it.obj.capacity())
            it.onRecycle {
                allocInUse.addAndGet(-it.obj.capacity())
            }
        }
    }

    /**
     * like [getByteBuffer] but it will put array into byte buffer
     */
    @JvmStatic
    fun getByteBuffer(array: ByteArray, isDirect: Boolean = false) : ReusableObject {
        val reusableObj = getByteBuffer(array.size, isDirect)
        reusableObj.get().also {
            // we don't need to clear buffer as it cleared when get
            it.put(array, 0, array.size)
            it.flip()
        }
        return reusableObj
    }

    override fun clean() {
        super.clean clean0@ {
            it.capacity() > MAX_BUFFER_SIZE
        }
    }

}