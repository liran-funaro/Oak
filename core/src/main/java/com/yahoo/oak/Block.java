/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class Block {

    // Anything lower than 1 byte is meaningless
    static final int MIN_BLOCK_SIZE = 1;
    // Integer is limited to 32 bits; one bit is for sign.
    static final int MAX_BLOCK_SIZE = 1 << (Integer.SIZE - 2);

    public static class CapacityExceeded extends Exception {
        CapacityExceeded(int capacity) {
            super(String.format("This block capacity was exceeded (capacity: %s).", capacity));
        }
    }

    public static final Block NULL = new Block();

    private final ByteBuffer buffer;

    private final int capacity;
    private final AtomicInteger allocated = new AtomicInteger(0);
    private int id; // placeholder might need to be set in the future

    Block(int capacity) {
        assert capacity > 0;
        this.capacity = capacity;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        this.buffer = ByteBuffer.allocateDirect(this.capacity);
    }

    // Only used to instantiate a null block
    private Block() {
        this.capacity = 0;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        this.buffer = null;
    }

    void setID(int id) {
        this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    // The returned buffer doesn't have all zero bytes.
    boolean allocate(Slice s, int size) throws CapacityExceeded {
        int now = allocated.getAndAdd(size);
        if (now + size > this.capacity) {
            allocated.getAndAdd(-size);
            throw new CapacityExceeded(capacity);
        }
        s.update(id, now, size);
        readByteBuffer(s);
        return true;
    }

    // use when this Block is no longer in any use, not thread safe
    // It sets the limit to the capacity and the position to zero, but didn't zeroes the memory
    void reset() {
        allocated.set(0);
    }

    // return how many bytes are actually allocated for this block only, thread safe
    long allocated() {
        return allocated.get();
    }

    // releasing the memory back to the OS, freeing the block, an opposite of allocation, not thread safe
    void clean() {
        if (buffer == null) {
            return;
        }

        Field cleanerField = null;
        try {
            cleanerField = buffer.getClass().getDeclaredField("cleaner");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        assert cleanerField != null;
        cleanerField.setAccessible(true);
        Cleaner cleaner = null;
        try {
            cleaner = (Cleaner) cleanerField.get(buffer);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        assert cleaner != null;
        cleaner.clean();
    }

    void readByteBuffer(Slice s) {
        s.setBuffer(buffer);
    }

    // how many bytes a block may include, regardless allocated/free
    public int getCapacity() {
        return capacity;
    }
}
