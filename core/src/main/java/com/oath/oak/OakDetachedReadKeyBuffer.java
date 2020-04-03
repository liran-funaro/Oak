/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.function.Function;

class OakDetachedReadKeyBuffer extends OakDetachedReadBuffer {

    final EntrySet.KeyBuffer key;

    OakDetachedReadKeyBuffer() {
        this.key = new EntrySet.KeyBuffer();
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
