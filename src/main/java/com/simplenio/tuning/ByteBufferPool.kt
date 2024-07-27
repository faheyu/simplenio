package com.simplenio.tuning

import java.nio.ByteBuffer

object ByteBufferPool : ObjectPool<ByteBuffer>() {

    private const val DEFAULT_BUFFER_SIZE = 8192

    fun getByteBuffer(capacity: Int = DEFAULT_BUFFER_SIZE) : ByteBuffer {
        val reusableByteBuffer = get {
            it.capacity() == capacity
        }?.also {
            it.clear()
        }
        return reusableByteBuffer ?: add(ByteBuffer.allocate(capacity), true)
    }

    fun getByteBuffer(array: ByteArray) : ByteBuffer {
        return getByteBuffer(array.size).also {
            it.clear()
            it.put(array, 0, array.size)
            it.flip()
        }
    }

    override fun clean(filter: ((ByteBuffer) -> Boolean)?) {
        super.clean {
            it.capacity() < DEFAULT_BUFFER_SIZE
        }
    }

}