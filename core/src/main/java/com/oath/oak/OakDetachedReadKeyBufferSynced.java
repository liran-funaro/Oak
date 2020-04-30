/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.function.Function;

/**
 * This class is used for when a detached access to the key is needed with synchronization.
 * It extends the non-synchronized version, and changes only the transformBuffer() method to perform synchronization
 * before any access to the data.
 * It is used by non-stream iterators that iterate over the keys (KeyIterator and EntryIterator).
 */
class OakDetachedReadKeyBufferSynced extends OakDetachedReadKeyBuffer {

    private final MemoryManager memoryManager;

    OakDetachedReadKeyBufferSynced(MemoryManager memoryManager) {
        super();
        this.memoryManager = memoryManager;
    }

    @Override
    protected <T> T transformBuffer(Function<OakAttachedReadBuffer, T> transformer) {
        // Internal call. No input validation.
        // No access is allowed once the memory manager is closed.
        // We avoid validating this here due to performance concerns.
        // The correctness is persevered because when the memory manager is closed,
        // its block array is no longer reachable.
        // Thus, a null pointer exception will be raised once we try to get the byte buffer.
        memoryManager.readByteBuffer(key);
        return transformer.apply(key);
    }
}
