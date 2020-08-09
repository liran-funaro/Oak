/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class NativeMemoryAllocator implements BlockMemoryAllocator {

    // When allocating n bytes and there are buffers in the free list, only free buffers of size <= n *
    // REUSE_MAX_MULTIPLIER will be recycled
    // This parameter may be tuned for performance vs off-heap memory utilization
    private static final int REUSE_MAX_MULTIPLIER = 2;
    public static final int INVALID_BLOCK_ID = 0;
    public static final int DEFAULT_MIN_BLOCK_SIZE = 1024;
    public static final int DEFAULT_MAX_BLOCK_SIZE = 256 * (1 << 20);

    // If selecting a huge capacity to this allocator, the blocks array size can be unjustifiably big.
    // To avoid limiting the size of the block array (and thus the capacity),
    // we limits its initial (and incremental) allocations.
    private static final int BLOCKS_ARRAY_MAX_ALLOCATION = 128;

    // mapping IDs to blocks allocated solely to this Allocator
    private Block[] blocksArray;
    // The ID generator is only accesses internally with a lock.
    private int idGenerator = 1;

    /**
     * free list of Slices which can be reused.
     * They are sorted by the slice length, then by the block id, then by their offset.
     * See {@code Slice.compareTo(Slice)} for more information.
     */
    private final ConcurrentSkipListSet<Slice> freeList = new ConcurrentSkipListSet<>();

    private final BlocksProvider blocksProvider;
    private Block currentBlock;

    // Memory allocation limit for this Allocator
    private final Long capacity;
    private final int minBlockSize;
    private final int maxBlockSize;
    private final int maxBlocksArraySize;

    private int nextBlockSize;

    // number of bytes allocated for this Oak among different Blocks
    // can be calculated, but kept for easy access
    private final AtomicLong allocated = new AtomicLong(0);
    private final AtomicLong blockAllocatedBytes = new AtomicLong(0);
    public final AtomicInteger keysAllocated = new AtomicInteger(0);
    public final AtomicInteger valuesAllocated = new AtomicInteger(0);

    // flag allowing not to close the same allocator twice
    private final AtomicBoolean closed = new AtomicBoolean(false);

    NativeMemoryAllocator() {
        this(null, null, null);
    }

    NativeMemoryAllocator(Long capacity) {
        this(capacity, null, null);
    }

    NativeMemoryAllocator(Long capacity, Integer fixedBlockSize) {
        this(capacity, fixedBlockSize, fixedBlockSize);
    }

    // constructor
    // input param: memory capacity given to this Oak. Uses default BlocksPool
    NativeMemoryAllocator(Long capacity, Integer minBlockSize, Integer maxBlockSize) {
        this(BlocksPool.getInstance(), capacity, minBlockSize, maxBlockSize);
    }

    // A testable constructor
    NativeMemoryAllocator(BlocksProvider blocksProvider, Long capacity, Integer minBlockSize, Integer maxBlockSize) {
        this.blocksProvider = blocksProvider;
        this.capacity = capacity;

        int selectedMinBlockSize = DEFAULT_MIN_BLOCK_SIZE;
        int selectedMaxBlockSize = DEFAULT_MAX_BLOCK_SIZE;

        long reqBlockArraySize = Integer.MAX_VALUE;

        // If min block size was provided, validate and round it.
        if (minBlockSize != null) {
            BlocksPool.validateBlockSize(minBlockSize);
            selectedMinBlockSize = BlocksPool.ceilingBlockSizePowerOf2(minBlockSize);
        }

        // If max block size was provided, validate and round it.
        if (maxBlockSize != null) {
            BlocksPool.validateBlockSize(maxBlockSize);
            selectedMaxBlockSize = BlocksPool.ceilingBlockSizePowerOf2(maxBlockSize);
        }

        // If capacity was provided, validate it.
        if (capacity != null) {
            if (capacity < 1) {
                throw new IllegalArgumentException(
                        String.format("The requested capacity must be greater than 1 " +
                                "(capacity: %s)", capacity)
                );
            }

            // If min-block-size was provided, then it must be grater than or equal to the capacity.
            if (minBlockSize != null && capacity < minBlockSize) {
                throw new IllegalArgumentException(
                        String.format("The requested capacity must be greater than the minimal block size " +
                                "(capacity: %s, minBlockSize: %s)", capacity, minBlockSize)
                );
            }

            // If min-block-size was not provided, then we can select it to fit our provided capacity.
            if (minBlockSize == null && capacity < selectedMinBlockSize) {
                selectedMinBlockSize = BlocksPool.ceilingBlockSizePowerOf2(capacity.intValue()) / 2;
            }

            // If capacity was set, we have to make sure that the provided block limits can attain this capacity.
            reqBlockArraySize = calculateRequiredBlocksArraySize(capacity, selectedMinBlockSize, selectedMaxBlockSize);

            // If not provided upper limit for the block size, then try larger blocks to fit the required capacity.
            if (maxBlockSize == null) {
                while (reqBlockArraySize > Integer.MAX_VALUE && selectedMaxBlockSize < Block.MAX_BLOCK_SIZE) {
                    selectedMaxBlockSize *= 2;
                    reqBlockArraySize = calculateRequiredBlocksArraySize(capacity,
                            selectedMinBlockSize, selectedMaxBlockSize);
                }
            }

            // If not provided lower limit for the block size, then try larger blocks to fit the required capacity.
            if (minBlockSize == null) {
                while (reqBlockArraySize > Integer.MAX_VALUE && selectedMinBlockSize < Block.MAX_BLOCK_SIZE) {
                    selectedMinBlockSize *= 2;
                    reqBlockArraySize = calculateRequiredBlocksArraySize(capacity,
                            selectedMinBlockSize, selectedMaxBlockSize);
                }
            }

            // If we couldn't find block limits to attain this capacity, then we should alert the user.
            if (reqBlockArraySize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        String.format("The requested capacity is unattainable with the provided block size limits " +
                                "(capacity: %s, min-block-size: %s, max-block-size: %s)",
                                capacity, minBlockSize, maxBlockSize)
                );
            }
        }

        this.minBlockSize = selectedMinBlockSize;
        this.maxBlockSize = selectedMaxBlockSize;
        this.maxBlocksArraySize = (int) reqBlockArraySize;

        this.blocksArray = new Block[Math.min(maxBlocksArraySize, BLOCKS_ARRAY_MAX_ALLOCATION)];
        this.currentBlock = Block.NULL;
        this.nextBlockSize = this.minBlockSize;
    }

    private static long calculateRequiredBlocksArraySize(long capacity, int minBlockSize, int maxBlockSize) {
        // The maximal capacity that can be reached when using only increasing blocks by a factor of 2 that starts
        // with minBlockSize and ends with maxBlockSize:
        //        minBlockSize + minBlockSize*2 + minBlockSize*4 + ... + maxBlockSize
        long maxFactorCapacity = ((long) maxBlockSize) * 2L - (long) minBlockSize;

        // Number of blocks increasing by a factor of 2 between the minimal and maximal block
        long blockArraySize = 1 +
                Integer.numberOfLeadingZeros(minBlockSize) - Integer.numberOfLeadingZeros(maxBlockSize);

        // It might be needed to add more blocks of the maxBlockSize to reach the capacity
        if (maxFactorCapacity < capacity) {
            blockArraySize += ((capacity - maxFactorCapacity) / maxBlockSize) + 1;
        }

        // first entry of blocksArray is always empty
        return blockArraySize + 1;
    }

    // Allocates ByteBuffer of the given size, either from freeList or (if it is still possible)
    // within current block bounds.
    // Otherwise, new block is allocated within Oak memory bounds. Thread safe.
    @Override
    public boolean allocate(Slice s, int size, MemoryManager.Allocate allocate) {
        // While the free list is not empty there can be a suitable free slice to reuse.
        // To search a free slice, we use the input slice as a dummy and change its length to the desired length.
        // Then, we use freeList.higher(s) which returns a free slice with greater or equal length to the length of the
        // dummy with time complexity of O(log N), where N is the number of free slices.
        while (!freeList.isEmpty()) {
            s.update(0, 0, size);
            Slice bestFit = freeList.higher(s);
            if (bestFit == null) {
                break;
            }
            // If the best fit is more than REUSE_MAX_MULTIPLIER times as big than the desired length, than a new
            // buffer is allocated instead of reusing.
            // This means that currently buffers are not split, so there is some internal fragmentation.
            if (bestFit.getAllocatedLength() > (REUSE_MAX_MULTIPLIER * size)) {
                break;     // all remaining buffers are too big
            }
            // If multiple threads got the same bestFit only one can use it (the one which succeeds in removing it
            // from the free list).
            // The rest restart the while loop.
            if (freeList.remove(bestFit)) {
                if (stats != null) {
                    stats.reclaim(size);
                }
                s.copyFrom(bestFit);
                return true;
            }
        }

        boolean isAllocated = false;
        // freeList is empty or there is no suitable slice
        while (!isAllocated) {
            try {
                isAllocated = currentBlock.allocate(s, size);
            } catch (Block.CapacityExceeded ignored) {
                // We haven't allocated any block yet or there is no space in current block.

                // First, lets check that we can allocate this size
                if (size < 1 || size > maxBlockSize) {
                    throw new IllegalArgumentException(
                            String.format("The required allocation must be between 1 to the max block size " +
                                    "(required: %s, max-block-size: %s).", size, maxBlockSize));
                }

                synchronized (this) {
                    // Going to allocate additional block.
                    // Need to be thread-safe, so no multiple blocks are allocated simultaneously.
                    // Locking is actually the most reasonable way of synchronization here
                    if (currentBlock.allocated() + size > currentBlock.getCapacity()) {
                        allocateNewCurrentBlock(size);
                    }
                }
            }
        }

        allocated.addAndGet(size);
        if (allocate == MemoryManager.Allocate.KEY) {
            keysAllocated.incrementAndGet();
        } else {
            valuesAllocated.incrementAndGet();
        }
        return true;
    }

    // Releases memory (makes it available for reuse) without other GC consideration.
    // Meaning this request should come while it is ensured none is using this memory.
    // Thread safe.
    // IMPORTANT: it is assumed free will get an allocation only initially allocated from this
    // Allocator!
    @Override
    public void free(Slice s) {
        int size = s.getAllocatedLength();
        allocated.addAndGet(-size);
        if (stats != null) {
            stats.release(size);
        }
        freeList.add(new Slice(s));
    }

    // Releases all memory allocated for this Oak (should be used as part of the Oak destruction)
    // Not thread safe, should be a single thread call. (?)
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Release the hold of the block array and return it the provider.
        Block[] b = blocksArray;
        blocksArray = null;

        // Reset "closed" to apply a memory barrier before actually returning the block.
        closed.set(true);

        for (int i = 1; i < idGenerator; i++) {
            blocksProvider.returnBlock(b[i]);
        }
        // no need to do anything with the free list,
        // as all free list members were residing on one of the (already released) blocks
    }

    // Returns the off-heap allocation of this OakMap
    @Override
    public long allocated() {
        return allocated.get();
    }

    public int getFreeListLength() {
        return freeList.size();
    }


    @Override
    public boolean isClosed() {
        return closed.get();
    }

    // When some buffer need to be read from a random block
    @Override
    public void readByteBuffer(Slice s) {
        // Validates that the input block id is valid.
        // This check should be automatically eliminated by the compiler in production.
        assert s.getAllocatedBlockID() > NativeMemoryAllocator.INVALID_BLOCK_ID :
                String.format("Invalid block-id: %s", s);
        Block b = blocksArray[s.getAllocatedBlockID()];
        b.readByteBuffer(s);
    }

    @Override
    public int getMaxBlockSize() {
        return maxBlockSize;
    }

    // used only for testing
    Block getCurrentBlock() {
        return currentBlock;
    }

    // used only for testing
    int numOfAllocatedBlocks() {
        return idGenerator - 1;
    }

    // This method MUST be called within a thread safe context.
    private void reallocateBlocksArray() {
        int newSize = Math.min(blocksArray.length + BLOCKS_ARRAY_MAX_ALLOCATION, maxBlocksArraySize);
        Block[] newBlocksArray = new Block[newSize];
        System.arraycopy(blocksArray, 0, newBlocksArray, 0, blocksArray.length);
        blocksArray = newBlocksArray;
    }

    private void validateCapacity(int requiredSize) {
        if (capacity == null) {
            return;
        }

        long remainingCapacity = capacity - blockAllocatedBytes.get();
        if (remainingCapacity >= nextBlockSize) {
            return;
        }

        // We don't have enough capacity for the next block size.
        // Lets select the minimal block size that could fit.
        nextBlockSize = Math.max(minBlockSize, BlocksPool.ceilingBlockSizePowerOf2(requiredSize));

        // If the remaining capacity is smaller than the required, we can't allocate anymore.
        if (remainingCapacity < nextBlockSize) {
            throw new OakOutOfMemoryException(
                    String.format("This allocator capacity was exceeded (capacity: %s).", capacity));
        }
    }

    // This method MUST be called within a thread safe context.
    // Its caller must validate that the required size is valid.
    private void allocateNewCurrentBlock(int requiredSize) {
        assert requiredSize <= maxBlockSize : "The caller must validate the required size.";

        if (nextBlockSize < requiredSize) {
            nextBlockSize = BlocksPool.ceilingBlockSizePowerOf2(requiredSize);
        }

        validateCapacity(requiredSize);

        Block b = blocksProvider.getBlock(nextBlockSize);
        int blockID = idGenerator++;
        if (blocksArray.length <= blockID) {
            reallocateBlocksArray();
        }
        this.blocksArray[blockID] = b;
        b.setID(blockID);
        this.currentBlock = b;
        blockAllocatedBytes.addAndGet(b.getCapacity());

        nextBlockSize = Math.min(maxBlockSize, nextBlockSize * BlocksPool.BLOCK_SIZE_BASE);

        // If we have some leftover capacity, keep it in the free list.
        // if (lastBlock != null && lastBlock.allocated() < lastBlock.getCapacity()) {
        //     try {
        //         Slice s = new Slice();
        //         lastBlock.allocate(s, (int) (lastBlock.getCapacity() - lastBlock.allocated()));
        //         freeList.add(s);
        //     } catch (OakOutOfMemoryException ignored) {}
        // }
    }

    private Stats stats = null;

    public void collectStats() {
        stats = new Stats();
    }

    public Stats getStats() {
        return stats;
    }

    static class Stats {
        int reclaimedBuffers;
        int releasedBuffers;
        long releasedBytes;
        long reclaimedBytes;

        public void release(int size) {
            synchronized (this) {
                releasedBuffers++;
                releasedBytes += size;
            }
        }

        public void reclaim(int size) {
            synchronized (this) {
                reclaimedBuffers++;
                reclaimedBytes += size;
            }
        }
    }

}


