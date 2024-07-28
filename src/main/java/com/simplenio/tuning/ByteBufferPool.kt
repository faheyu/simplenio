package com.simplenio.tuning

import com.simplenio.DebugLogger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger

object ByteBufferPool : ObjectPool<ByteBuffer>() {

    private val logger = DebugLogger(ByteBufferPool::class.java.simpleName)

    private const val MIN_BUFFER_SIZE = 512
    private val allocated = AtomicInteger()

    fun getByteBuffer(capacity: Int, isDirect: Boolean = true) : ReusableObject {
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
        byteBuffer.clear().limit(capacity).order(ByteOrder.nativeOrder())
        return reusableByteBuffer
    }

    fun getByteBuffer(array: ByteArray, isDirect: Boolean = true) : ReusableObject {
        val reusableObj = getByteBuffer(array.size, isDirect)
        reusableObj.get().also {
            it.clear()
            it.put(array, 0, array.size)
            it.flip()
        }
        return reusableObj
    }

    override fun clean() {
        super.clean clean0@ {
            if (it.isDirect)
                return@clean0 false

            (it.capacity() < MIN_BUFFER_SIZE).also { shouldClean ->
                // subtract clean capacity
                if (shouldClean) {
                    allocated.addAndGet(-it.capacity())
                }
            }
        }
        logger.d("cleaned, total allocated size: $allocated")
    }

}