/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;


public class BlocksPoolTest {

    public static void releaseBlock(BlocksPool pool, Block b, int expectedSize,
                                    long expectedAlloc, long expectedCached) {
        Assert.assertEquals(expectedSize, b.getCapacity());
        pool.returnBlock(b);

        Assert.assertEquals(expectedAlloc, pool.getAllocatedBytes());
        Assert.assertEquals(expectedCached, pool.getCachedBytes());
    }

    @Test
    public void testPoolRelease() {
        final int l = 8;
        final int h = 31;

        BlocksPool pool = new BlocksPool(1L << l, 1L << (h-1));
        LinkedList<Block> q = new LinkedList<>();

        Block b = pool.getBlock(1 << l);
        q.addLast(b);

        for (int i = l; i < h; i++) {
            int size = 1 << i;
            b = pool.getBlock(size);
            q.addLast(b);

            // We release the buffer so this test won't overuse the memory
            b.clean();
        }

        Assert.assertEquals(1L << h, pool.getAllocatedBytes());
        Assert.assertEquals(0, pool.getCachedBytes());

        // Release the largest block (it should be cached)
        releaseBlock(pool, q.pollLast(), 1 << (h-1),
                1L << h, 1L << (h-1));

        // Release the smallest block (it should be cached instead of the larger one)
        releaseBlock(pool, q.pollFirst(), 1 << l,
                1L << (h-1), 1L << l);

        // Release all blocks (all of them should be cached)
        while (!q.isEmpty()) {
            pool.returnBlock(q.pollFirst());
        }

        Assert.assertEquals(1L << (h-1), pool.getAllocatedBytes());
        Assert.assertEquals(1L << (h-1), pool.getCachedBytes());

        // Allocate additional block
        b = pool.getBlock(1 << (h-1));
        q.addLast(b);

        Assert.assertEquals(1L << h, pool.getAllocatedBytes());
        Assert.assertEquals(1L << (h-1), pool.getCachedBytes());

        // Release the block (should trigger a cleanup)
        while (!q.isEmpty()) {
            pool.returnBlock(q.pollFirst());
        }

        Assert.assertEquals(1 << l, pool.getAllocatedBytes());
        Assert.assertEquals(1 << l, pool.getCachedBytes());

        pool.close();
    }
}
