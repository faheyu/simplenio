package com.simplenio.util

import java.nio.ByteBuffer

/**
 * Put the [byteBuffer] to this byte buffer, expand if this byte buffer can not put all [byteBuffer].
 * We need to use the returned byte buffer to ensure using the expanded one.
 *
 * @param byteBuffer the byte buffer to put to this byte buffer
 * @param size the size to expand if this byte buffer can not put all the [byteBuffer].
 * The new buffer's capacity is not larger than multiple of [size].
 *
 * @return this byte buffer or the new expanded byte buffer
 */
fun ByteBuffer.putAndExpandIfNeeded(byteBuffer: ByteBuffer, size: Int) : ByteBuffer {
    if (remaining() < byteBuffer.limit()) {
        /**
         * current position + remaining = limit
         * but remaining < byteBuffer.limit()
         * so new limit is current position + byteBuffer's limit
         */
        val newLimit = position() + byteBuffer.limit()

        /**
         * ensure new capacity is not less than multiple of size
         */
        val newCapacity = (newLimit / size + 1) * size
        val newBuffer = ByteBuffer.allocate(newCapacity)

        // transfer data
        flip()
        newBuffer.put(this)
        newBuffer.put(byteBuffer)
        return newBuffer
    }

    // no need to expand, just put it normally
    put(byteBuffer)
    return this
}