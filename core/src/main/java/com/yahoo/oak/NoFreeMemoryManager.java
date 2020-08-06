/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


class NoFreeMemoryManager implements MemoryManager {
    private final BlockMemoryAllocator keysMemoryAllocator;
    private final BlockMemoryAllocator valuesMemoryAllocator;

    NoFreeMemoryManager(BlockMemoryAllocator memoryAllocator) {
        assert memoryAllocator != null;

        this.valuesMemoryAllocator = memoryAllocator;
        this.keysMemoryAllocator = memoryAllocator;
    }

    public void close() {
        valuesMemoryAllocator.close();
        keysMemoryAllocator.close();
    }

    public long allocated() {
        return valuesMemoryAllocator.allocated();
    }

    @Override
    public void allocate(Slice s, int size, Allocate allocate) {
        boolean allocated = keysMemoryAllocator.allocate(s, size, allocate);
        assert allocated;
    }

    @Override
    public void release(Slice s) {
    }

    @Override
    public void readByteBuffer(Slice s) {
        keysMemoryAllocator.readByteBuffer(s);
    }

    public boolean isClosed() {
        return keysMemoryAllocator.isClosed() || valuesMemoryAllocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public int getMaxBlockSize() {
        return keysMemoryAllocator.getMaxBlockSize();
    }
}

