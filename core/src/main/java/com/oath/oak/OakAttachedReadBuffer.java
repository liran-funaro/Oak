/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class OakAttachedReadBuffer extends Slice implements OakReadBuffer, OakUnsafeDirectBuffer {

    protected final int headerSize;

    OakAttachedReadBuffer(int headerSize) {
        this.headerSize = headerSize;
    }

    OakAttachedReadBuffer(Slice alloc, int headerSize) {
        super(alloc);
        this.headerSize = headerSize;
    }

    protected void checkIndex(int index) {
        if (index < 0 || index >= getDataLength()) {
            throw new IndexOutOfBoundsException();
        }
    }

    protected int getDataOffset(int index) {
        checkIndex(index);
        // Slice.offset points to the header.
        // The user's data offset is: offset + headerSize
        return getAllocOffset(headerSize + index);
    }

    protected int getDataLength() {
        // Slice.length includes the header size.
        // The user's data length is: length - headerSize
        return getAllocLength(headerSize);
    }

    @Override
    public int capacity() {
        return getDataLength();
    }

    @Override
    public ByteOrder order() {
        return buffer.order();
    }

    @Override
    public byte get(int index) {
        return getAllocByteBuffer(headerSize).get(getDataOffset(index));
    }

    @Override
    public char getChar(int index) {
        return getAllocByteBuffer(headerSize).getChar(getDataOffset(index));
    }

    @Override
    public short getShort(int index) {
        return getAllocByteBuffer(headerSize).getShort(getDataOffset(index));
    }

    @Override
    public int getInt(int index) {
        return getAllocByteBuffer(headerSize).getInt(getDataOffset(index));
    }

    @Override
    public long getLong(int index) {
        return getAllocByteBuffer(headerSize).getLong(getDataOffset(index));
    }

    @Override
    public float getFloat(int index) {
        return getAllocByteBuffer(headerSize).getFloat(getDataOffset(index));
    }

    @Override
    public double getDouble(int index) {
        return getAllocByteBuffer(headerSize).getDouble(getDataOffset(index));
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

    @Override
    public ByteBuffer getByteBuffer() {
        return getAllocReadByteBuffer(headerSize);
    }

    @Override
    public int getOffset() {
        return getAllocOffset(headerSize);
    }

    @Override
    public int getLength() {
        return getDataLength();
    }

    @Override
    public long getAddress() {
        return getAllocAddress(headerSize);
    }
}
