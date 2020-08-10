/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * The singleton Pool to pre-allocate and reuse blocks of off-heap memory. The singleton has lazy
 * initialization so the big memory is allocated only on demand when first Oak is used.
 * However it makes creation of the first Oak slower. This initialization is thread safe, thus
 * multiple concurrent Oak creations will result only in the one Pool.
 */
final class BlocksPool implements BlocksProvider, Closeable {

    private static BlocksPool instance = null;

    // Blocks can only be allocated as a power of two.
    public static final int BLOCK_SIZE_BASE = 2;

    static final long GB = 1L << 30;

    // TODO change the following constants to be configurable

    // Upper/lower thresholds to the quantity of the cached blocks to reserve in the pool for future use.
    // When the cached memory quantity reaches upperCacheThresholdBytes, some blocks are freed
    // such that the remaining cached memory will be at most lowerCacheThresholdBytes.
    private static final long DEFAULT_LOWER_CACHE_THRESHOLD_BYTES = 2L * GB;
    private static final long DEFAULT_UPPER_CACHE_THRESHOLD_BYTES = 4L * GB;

    private final long lowerCacheThresholdBytes;
    private final long upperCacheThresholdBytes;
    private final ConcurrentLinkedQueue<Block>[] blockPools;

    private final AtomicLong allocatedBytes = new AtomicLong(0);
    private final AtomicLong cachedBytes = new AtomicLong(0);

    BlocksPool() {
        this(DEFAULT_LOWER_CACHE_THRESHOLD_BYTES, DEFAULT_UPPER_CACHE_THRESHOLD_BYTES);
    }

    // Used internally and for tests.
    BlocksPool(long lowerCacheThresholdBytes, long upperCacheThresholdBytes) {
        this.lowerCacheThresholdBytes = lowerCacheThresholdBytes;
        this.upperCacheThresholdBytes = upperCacheThresholdBytes;
        this.blockPools = new ConcurrentLinkedQueue[Integer.SIZE];
        for (int i = 0; i < blockPools.length; i++) {
            blockPools[i] = new ConcurrentLinkedQueue<>();
        }
    }

    /**
     * Initializes the instance of BlocksPool if not yet initialized, otherwise returns
     * the single instance of the singleton. Thread safe.
     */
    static BlocksPool getInstance() {
        if (instance == null) {
            synchronized (BlocksPool.class) { // can be easily changed to lock-free
                if (instance == null) {
                    instance = new BlocksPool();
                }
            }
        }
        return instance;
    }

    /**
     * Taken from: org.apache.datasketches.Util
     * Computes the ceiling power of 2 within the range [1, 2^30]. This is the smallest positive power
     * of 2 that equal to or greater than the given n and equal to a mathematical integer.
     *
     * @param n The input argument.
     * @return the ceiling power of 2.
     */
    public static int ceilingBlockSizePowerOf2(final int n) {
        return Integer.highestOneBit((n - 1) << 1);
    }

    public static void validateBlockSize(final int n) {
        if (n < Block.MIN_BLOCK_SIZE || n > Block.MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Illegal block size (out of limits: %s-%s): %s",
                            Block.MIN_BLOCK_SIZE, Block.MAX_BLOCK_SIZE, n));
        }
    }

    /**
     * Returns a single Block from within the Pool, enlarges the Pool if needed
     * Thread-safe.
     * The caller must validate that the requested size is within the block limits and is a power of two.
     */
    @Override
    public Block getBlock(int requiredSize) {
        // Validate that this block pool is always requested valid block sizes
        assert requiredSize >= Block.MIN_BLOCK_SIZE && requiredSize <= Block.MAX_BLOCK_SIZE;
        assert requiredSize == ceilingBlockSizePowerOf2(requiredSize);

        // poolNum is a number between 0 to 31 due to the limitation of Integer.
        // 0 corresponds to the largest block size, and 31 to the smallest.
        int poolNum = Integer.numberOfLeadingZeros(requiredSize);
        ConcurrentLinkedQueue<Block> pool = blockPools[poolNum];

        Block b = pool.poll();
        if (b != null) {
            cachedBytes.addAndGet(-b.getCapacity());
        } else {
            // The blocks are allocated without ids.
            // They are given an id when they are given to an OakNativeMemoryAllocator.
            b = new Block(requiredSize);
            allocatedBytes.addAndGet(requiredSize);
        }
        return b;
    }

    private synchronized void cleanup() {
        if (cachedBytes.get() <= upperCacheThresholdBytes) { // too many cached blocks
            return;
        }

        // Iteration order is from the largest to the smallest buffer
        for (ConcurrentLinkedQueue<Block> pool : blockPools) {
            while (!pool.isEmpty()) {
                if (cachedBytes.get() <= lowerCacheThresholdBytes) {
                    return;
                }

                Block releasedBlock = pool.poll();
                releasedBlock.clean();
                int releasedBlockSize = releasedBlock.getCapacity();
                cachedBytes.addAndGet(-releasedBlockSize);
                allocatedBytes.addAndGet(-releasedBlockSize);
            }
        }
    }

    /**
     * Returns a single Block to the Pool, decreases the Pool if needed
     * Assumes block is not used by any concurrent thread, otherwise thread-safe
     */
    @Override
    public void returnBlock(Block b) {
        b.reset();

        int blockSize = b.getCapacity();

        int poolNum = Integer.numberOfLeadingZeros(blockSize);
        ConcurrentLinkedQueue<Block> curPool = blockPools[poolNum];

        curPool.add(b);
        cachedBytes.addAndGet(blockSize);

        cleanup();
    }

    /**
     * Should be called when the entire Pool is not used anymore. Releases the memory only of the
     * blocks returned back to the pool.
     * However, this object is GCed when the entire process dies, and thus all the memory is released
     * anyway.
     */
    @Override
    public void close() {
        for (ConcurrentLinkedQueue<Block> pool : blockPools) {
            while (!pool.isEmpty()) {
                pool.poll().clean();
            }
        }

        cachedBytes.set(0);
        allocatedBytes.set(0);
    }

    // used only for testing
    int numOfRemainingBlocks() {
        return Stream.of(blockPools).mapToInt(ConcurrentLinkedQueue::size).sum();
    }

    long getAllocatedBytes() {
        return allocatedBytes.get();
    }

    long getCachedBytes() {
        return cachedBytes.get();
    }
}
