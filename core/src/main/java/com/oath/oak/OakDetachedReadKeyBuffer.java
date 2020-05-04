/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.function.Function;

/**
 * This class is used for when a detached access to the key is needed without synchronization.
 * It does not have to acquire a read lock before each access.
 * Thus, it can only be used without other concurrent writes in the background.
 * It is used by stream iterators that iterate over the keys (KeyStreamIterator and EntryStreamIterator).
 */
class OakDetachedReadKeyBuffer extends OakDetachedReadBuffer {

    final KeyBuffer key;

    OakDetachedReadKeyBuffer() {
        this.key = new KeyBuffer();
    }

    @Override
    protected OakAttachedReadBuffer getAttachedBuffer() {
        return key;
    }

    @Override
    protected <T> T transformBuffer(Function<OakAttachedReadBuffer, T> transformer) {
        // Internal call. No input validation.
        return transformer.apply(key);
    }
}
