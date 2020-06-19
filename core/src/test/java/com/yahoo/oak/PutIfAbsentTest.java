/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class PutIfAbsentTest {
    private OakMap<Integer, Integer> oak;
    private CountDownLatch startSignal;
    private List<Future<Integer>> threads;
    private static final int NUM_THREADS = 31;
    private static final int NUM_KEYS = 100000;

    @Before
    public void init() {
        OakMapBuilder<Integer, Integer> builder = OakCommonBuildersFactory.getDefaultIntBuilder();
        oak = builder.build();
        startSignal = new CountDownLatch(1);
        threads = new ArrayList<>(NUM_THREADS);
    }

    @After
    public void finish() {
        oak.close();
    }


    @Test(timeout = 60_000)
    public void testConcurrentPutOrCompute() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; ++i) {
            Callable<Integer> operation = () -> {
                int counter = 0;
                try {
                    startSignal.await();

                    for (int j = 0; j < NUM_KEYS; ++j) {
                        boolean retval = oak.zc().putIfAbsentComputeIfPresent(j, 1, buffer -> {
                            int currentVal = buffer.getInt(0);
                            buffer.putInt(0, currentVal + 1);
                        });
                        if (retval) {
                            counter++;
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counter;
            };

            Future<Integer> future = executor.submit(operation);
            threads.add(future);
        }

        startSignal.countDown();
        final int[] returnValues = {0};
        threads.forEach(t -> {
            try {
                returnValues[0] += t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                Assert.fail();
            }
        });

        Iterator<Integer> iterator = oak.values().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Integer value = iterator.next();
            Assert.assertEquals((Integer) NUM_THREADS, value);
            count2++;
        }
        Assert.assertEquals(count2, NUM_KEYS);
        Assert.assertEquals(NUM_KEYS, oak.size());
        Assert.assertEquals(NUM_KEYS, returnValues[0]);
    }


    @Test(timeout = 60_000)
    public void testConcurrentPutIfAbsent() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; ++i) {
            Callable<Integer> operation = () -> {
                int counter = 0;
                try {
                    startSignal.await();

                    for (int j = 0; j < NUM_KEYS; ++j) {
                        boolean retval = oak.zc().putIfAbsent(j, j);
                        if (retval) {
                            counter++;
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counter;
            };

            Future<Integer> future = executor.submit(operation);
            threads.add(future);
        }

        startSignal.countDown();
        final int[] returnValues = {0};
        threads.forEach(t -> {
            try {
                returnValues[0] += t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                Assert.fail();
            }
        });

        Iterator<Map.Entry<Integer, Integer>> iterator = oak.entrySet().iterator();
        int count2 = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            Assert.assertEquals(entry.getKey(), entry.getValue());
            count2++;
        }
        Assert.assertEquals(count2, NUM_KEYS);
        Assert.assertEquals(NUM_KEYS, returnValues[0]);
        Assert.assertEquals(NUM_KEYS, oak.size());
    }
}
