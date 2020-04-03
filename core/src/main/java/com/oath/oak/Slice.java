/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

// Represents a portion of a bigger block which is part of the underlying managed memory.
// It is allocated via block memory allocator, and can be de-allocated later
class Slice {
    public static final int INVALID_VERSION = 0;

    protected int blockID;
    protected int offset;
    protected int length;

    // Allocation time version
    protected int version;

    protected ByteBuffer buffer;

    public Slice() {
        invalidate();
    }

    // Used to duplicate the allocation state. Does not duplicate the buffer itself.
    Slice(Slice otherSlice) {
        copyFrom(otherSlice);
    }

    // Reset to invalid state
    void invalidate() {
        blockID = OakNativeMemoryAllocator.INVALID_BLOCK_ID;
        offset = -1;
        length = -1;
        version = INVALID_VERSION;
        buffer = null;
    }

    boolean isValid() {
        return blockID != OakNativeMemoryAllocator.INVALID_BLOCK_ID;
    }

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length) {
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;

        // Invalidate the buffer and version. Will be assigned by the allocator.
        this.version = INVALID_VERSION;
        this.buffer = null;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    // Copy the block allocation information from another block allocation.
    void copyFrom(Slice alloc) {
        if (alloc == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = alloc.blockID;
        this.offset = alloc.offset;
        this.length = alloc.length;
        this.version = alloc.version;
        this.buffer = alloc.buffer;
    }

    // Used by OffHeapList in "synchrobench" module, and for testings.
    Slice duplicateBuffer() {
        buffer = buffer.duplicate();
        return this;
    }

    /* ------------------------------------------------------------------------------------
     * Getters
     * ------------------------------------------------------------------------------------*/

    public int getAllocBlockID() {
        return blockID;
    }

    public ByteBuffer getAllocByteBuffer() {
        return getAllocByteBuffer(0);
    }

    public ByteBuffer getAllocByteBuffer(int additionalOffset) {
        buffer.limit(getAllocLimit());
        buffer.position(getAllocOffset(additionalOffset));
        return buffer;
    }

    public ByteBuffer getAllocDuplicatedByteBuffer() {
        return getAllocDuplicatedByteBuffer(0);
    }

    public ByteBuffer getAllocDuplicatedByteBuffer(int additionalOffset) {
        ByteBuffer dupBuffer = buffer.duplicate();
        dupBuffer.limit(getAllocLimit());
        dupBuffer.position(getAllocOffset(additionalOffset));
        return dupBuffer;
    }

    public ByteBuffer getAllocReadByteBuffer() {
        return getAllocReadByteBuffer(0);
    }

    public ByteBuffer getAllocReadByteBuffer(int additionalOffset) {
        ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
        readBuffer.limit(getAllocLimit());
        readBuffer.position(getAllocOffset(additionalOffset));
        return readBuffer;
    }

    public long getAllocAddress() {
        return getAllocAddress(0);
    }

    public long getAllocAddress(int additionalOffset) {
        return ((DirectBuffer) buffer).address() + getAllocOffset(additionalOffset);
    }

    public int getAllocOffset() {
        return getAllocOffset(0);
    }

    public int getAllocOffset(int additionalOffset) {
        assert additionalOffset <= length;
        return offset + additionalOffset;
    }

    public int getAllocLength() {
        return getAllocLength(0);
    }

    public int getAllocLength(int additionalOffset) {
        assert additionalOffset <= length;
        return length - additionalOffset;
    }

    public int getAllocLimit() {
        return offset + length;
    }

    // Set the version. Should be used by the memory allocator.
    void setAllocVersion(int version) {
        this.version = version;
    }

    public int getAllocVersion() {
        return version;
    }

    public boolean isValidVersion() {
        return version != INVALID_VERSION;
    }
}
