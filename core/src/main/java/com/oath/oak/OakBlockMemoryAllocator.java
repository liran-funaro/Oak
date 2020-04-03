/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

// this is the interface to be implemented to replace the OakNativeMemoryAllocator
// this is about allocation of a new block allocation (which has inside it a DirectByteBuffer in order to continue
// supporting off-heap memory).
// allocator is also getting a block allocation to reuse the memory, given this allocation is no longer in use by any
// thread.
// Note that block allocations cannot be merged into a single allocation, and an allocation currently is not split.

interface OakBlockMemoryAllocator {

    // Allocates a portion of a block of the given size, thread safe.
    boolean allocate(Slice s, int size, MemoryManager.Allocate allocate);

    // Releases a portion of a block (makes it available for reuse) without other GC consideration.
    // IMPORTANT: it is assumed free will get an allocation only initially allocated from this Allocator!
    void free(Slice s);

    // Is invoked when entire OakMap is closed
    void close();

    // Returns the memory allocation of this OakMap (this Allocator)
    long allocated();

    // Read the buffer from an allocation object that its parameters are set: blockID, offset and length
    void readByteBuffer(Slice s);

    // Check if this Allocator was already closed
    boolean isClosed();
}