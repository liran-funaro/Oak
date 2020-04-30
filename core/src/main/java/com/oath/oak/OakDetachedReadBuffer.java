/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

/**
 * This is an abstract class for detached buffers.
 * A class needs to implement its two abstract, protected methods that are used
 * internally by this abstract class.
 *  - OakAttachedReadBuffer getAttachedBuffer()
 *  - <T> T transformBuffer(Function<OakAttachedReadBuffer, T> transformer)
 * See the methods documentation below for more information.
 */
abstract class OakDetachedReadBuffer implements OakDetachedBuffer, OakUnsafeDirectBuffer {

    // capacity method does not require accessing the buffer, so no need for atomic operation.
    @Override
    public int capacity() {
        return getAttachedBuffer().capacity();
    }

    @Override
    public ByteOrder order() {
        return transformBuffer(OakReadBuffer::order);
    }

    @Override
    public byte get(int index) {
        return transformBuffer(buffer -> buffer.get(index));
    }

    @Override
    public char getChar(int index) {
        return transformBuffer(buffer -> buffer.getChar(index));
    }

    @Override
    public short getShort(int index) {
        return transformBuffer(buffer -> buffer.getShort(index));
    }

    @Override
    public int getInt(int index) {
        return transformBuffer(buffer -> buffer.getInt(index));
    }

    @Override
    public long getLong(int index) {
        return transformBuffer(buffer -> buffer.getLong(index));
    }

    @Override
    public float getFloat(int index) {
        return transformBuffer(buffer -> buffer.getFloat(index));
    }

    @Override
    public double getDouble(int index) {
        return transformBuffer(buffer -> buffer.getDouble(index));
    }

    /**
     * Returns a transformation of ByteBuffer content.
     *
     * @param transformer the function that executes the transformation
     * @return a transformation of the ByteBuffer content
     * @throws NullPointerException if the transformer is null
     */
    public <T> T transform(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        return transformBuffer(buffer -> transformer.apply(buffer.getAllocReadByteBuffer(buffer.headerSize).slice()));
    }

    /**
     * Returned the inner attached buffer without any validation.
     * It is used internally by this abstract class for when the internal buffer is not accessed, but only the
     * allocation attributes are used (length, capacity, offset, limit, etc.).
     */
    abstract protected OakAttachedReadBuffer getAttachedBuffer();

    /**
     * Apply a transformation on the inner attached buffer atomically.
     * It is used internally by this abstract class for when we use the internal buffer to read the data.
     * If the child class needs synchronization before accessing the data, it should implement the synchronization
     * in this method, surrounding the call for the transformer function.
     */
    abstract protected <T> T transformBuffer(Function<OakAttachedReadBuffer, T> transformer);

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        return transformBuffer(OakAttachedReadBuffer::getByteBuffer);
    }

    // Offset method does not require accessing the buffer, so no need for atomic operation.
    @Override
    public int getOffset() {
        return getAttachedBuffer().getOffset();
    }

    // Length method does not require accessing the buffer, so no need for atomic operation.
    @Override
    public int getLength() {
        return getAttachedBuffer().getLength();
    }

    @Override
    public long getAddress() {
        return transformBuffer(OakAttachedReadBuffer::getAddress);
    }
}
