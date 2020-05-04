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

    /**
     * An allocated slice might have reserved space for meta-data, i.e., a header.
     * In the current implementation, the header size is defined externally at the construction.
     * In future implementations, the header size should be part of the allocation and defined
     * by the allocator/memory-manager using the update() method.
     */
    protected final int headerSize;

    protected int blockID;
    protected int offset;
    protected int length;

    // Allocation time version
    protected int version;

    protected ByteBuffer buffer;

    public Slice(int headerSize) {
        this.headerSize = headerSize;
        invalidate();
    }

    // Should be used only for testing
    public Slice() {
        this(0);
    }

    // Used to duplicate the allocation state. Does not duplicate the buffer itself.
    Slice(Slice otherSlice) {
        this.headerSize = otherSlice.headerSize;
        copyFrom(otherSlice);
    }

    // Used by OffHeapList in "synchrobench" module, and for testings.
    Slice duplicateBuffer() {
        buffer = buffer.duplicate();
        return this;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata Setters
     * ------------------------------------------------------------------------------------*/

    // Reset to invalid state
    void invalidate() {
        blockID = OakNativeMemoryAllocator.INVALID_BLOCK_ID;
        offset = -1;
        length = -1;
        version = EntrySet.INVALID_VERSION;
        buffer = null;
    }

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length) {
        assert headerSize <= length;

        this.blockID = blockID;
        this.offset = offset;
        this.length = length;

        // Invalidate the buffer and version. Will be assigned by the allocator.
        this.version = EntrySet.INVALID_VERSION;
        this.buffer = null;
    }

    // Copy the block allocation information from another block allocation.
    void copyFrom(Slice other) {
        if (other == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = other.blockID;
        this.offset = other.offset;
        this.length = other.length;
        this.version = other.version;
        this.buffer = other.buffer;
        assert this.headerSize == other.headerSize;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    // Set the version. Should be used by the memory allocator.
    void setAllocVersion(int version) {
        this.version = version;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata Getters
     * ------------------------------------------------------------------------------------*/

    boolean isValid() {
        return blockID != OakNativeMemoryAllocator.INVALID_BLOCK_ID;
    }

    int getAllocBlockID() {
        return blockID;
    }

    int getAllocOffset() {
        return offset;
    }

    int getAllocLength() {
        return length;
    }

    int getAllocVersion() {
        return version;
    }

    boolean isValidVersion() {
        return version != EntrySet.INVALID_VERSION;
    }

    long getAllocAddress() {
        return ((DirectBuffer) buffer).address() + offset;
    }

    int getAllocLimit() {
        return offset + length;
    }

    private ByteBuffer getInternalByteBuffer(ByteBuffer buffer, int offset) {
        buffer.limit(getAllocLimit());
        buffer.position(offset);
        return buffer;
    }

    ByteBuffer getAllocByteBuffer() {
        return getInternalByteBuffer(buffer, offset);
    }

    /* ------------------------------------------------------------------------------------
     * Data Getters
     * ------------------------------------------------------------------------------------*/

    int getDataOffset() {
        return offset + headerSize;
    }

    int getDataLength() {
        return length - headerSize;
    }

    long getDataAddress() {
        return ((DirectBuffer) buffer).address() + getDataOffset();
    }

    ByteBuffer getDataByteBuffer() {
        return getInternalByteBuffer(buffer, getDataOffset());
    }

    ByteBuffer getDataDuplicatedReadByteBuffer() {
        return getInternalByteBuffer(buffer.asReadOnlyBuffer(), getDataOffset());
    }

    ByteBuffer getDataDuplicatedWriteByteBuffer() {
        return getInternalByteBuffer(buffer.duplicate(), getDataOffset());
    }
}
