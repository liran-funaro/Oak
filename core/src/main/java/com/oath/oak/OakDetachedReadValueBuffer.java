/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.util.function.Function;

class OakDetachedReadValueBuffer extends OakDetachedReadKeyBuffer {

    final EntrySet.ValueBuffer value;

    OakDetachedReadValueBuffer(int headerSize) {
        super();
        this.value = new EntrySet.ValueBuffer(headerSize);
    }

    @Override
    protected OakAttachedReadBuffer getAttachedBuffer() {
        return value;
    }

    @Override
    protected <T> T transformBuffer(Function<OakAttachedReadBuffer, T> transformer) {
        // Internal call. No input validation.
        return transformer.apply(value);
    }
}
