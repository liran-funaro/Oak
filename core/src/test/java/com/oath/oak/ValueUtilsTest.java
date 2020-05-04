package com.oath.oak;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.oath.oak.ValueUtils.ValueResult.*;
import static org.junit.Assert.*;

public class ValueUtilsTest {
    private NovaManager novaManager;
    private final ValueUtils valueOperator = new ValueUtilsImpl();
    private final Result r = new Result();
    private ThreadContext ctx;
    private ValueBuffer s;

    @Before
    public void init() {
        novaManager = new NovaManager(new OakNativeMemoryAllocator(128));
        ctx = new ThreadContext(0, valueOperator);
        s = ctx.value;
        novaManager.allocate(s, 20, MemoryManager.Allocate.VALUE);
        putInt(0, 1);
        valueOperator.initHeader(s);
    }

    private void putInt(int index, int value) {
        s.getAllocByteBuffer().putInt(s.getAllocOffset() + index, value);
    }

    private int getInt(int index) {
        return s.getAllocByteBuffer().getInt(s.getAllocOffset() + index);
    }

    @Test
    public void transformTest() {
        putInt(8, 10);
        putInt(12, 20);
        putInt(16, 30);

        Result result = valueOperator.transform(r, s,
                byteBuffer -> byteBuffer.getInt(0) + byteBuffer.getInt(4) + byteBuffer.getInt(8));
        assertEquals(TRUE, result.operationResult);
        assertEquals(60, ((Integer) result.value).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformUpperBoundTest() {
        valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(12));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformLowerBoundTest() {
        valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(-4));
    }

    @Test(timeout = 5000)
    public void cannotTransformWriteLockedTest() throws InterruptedException {
        Random random = new Random();
        final int randomValue = random.nextInt();
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread transformer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Result result = valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(4));
            assertEquals(TRUE, result.operationResult);
            assertEquals(randomValue, ((Integer) result.value).intValue());
        });
        assertEquals(TRUE, valueOperator.lockWrite(s));
        transformer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(12, randomValue);
        valueOperator.unlockWrite(s);
        transformer.join();
    }

    @Test
    public void multipleConcurrentTransformsTest() {
        putInt(8, 10);
        putInt(12, 14);
        putInt(16, 18);
        final int parties = 4;
        CyclicBarrier barrier = new CyclicBarrier(parties);
        Thread[] threads = new Thread[parties];
        for (int i = 0; i < parties; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                int index = new Random().nextInt(3) * 4;
                Result result = valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(index));
                assertEquals(TRUE, result.operationResult);
                assertEquals(10 + index, ((Integer) result.value).intValue());
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void cannotTransformDeletedTest() {
        valueOperator.deleteValue(s);
        Result result = valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(0));
        assertEquals(FALSE, result.operationResult);
    }

    @Test
    public void cannotTransformedDifferentVersionTest() {
        s.setAllocVersion(2);
        Result result = valueOperator.transform(r, s, byteBuffer -> byteBuffer.getInt(0));
        assertEquals(RETRY, result.operationResult);
    }

    @Test
    public void putWithNoResizeTest() {
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        assertEquals(TRUE, valueOperator.put(null, ctx, 10, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                for (int randomValue : randomValues) {
                    targetBuffer.putInt(randomValue);
                }
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager, null));
        assertEquals(randomValues[0], getInt(8));
        assertEquals(randomValues[1], getInt(12));
        assertEquals(randomValues[2], getInt(16));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putUpperBoundTest() {
        valueOperator.put(null, ctx, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(12, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager, null);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putLowerBoundTest() {
        valueOperator.put(null, ctx, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(-4, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, novaManager, null);
    }

    @Test
    public void cannotPutReadLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            valueOperator.put(null, ctx, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int randomValue : randomValues) {
                        targetBuffer.putInt(randomValue);
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, novaManager, null);
        });
        valueOperator.lockRead(s);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int a = getInt(8), b = getInt(12), c = getInt(16);
        valueOperator.unlockRead(s);
        putter.join();
        assertNotEquals(randomValues[0], a);
        assertNotEquals(randomValues[1], b);
        assertNotEquals(randomValues[2], c);
    }

    @Test
    public void cannotPutWriteLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0] - 1);
        putInt(12, randomValues[1] - 1);
        putInt(16, randomValues[2] - 1);
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            valueOperator.put(null, ctx, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int i = 0; i < targetBuffer.remaining(); i += 4) {
                        assertEquals(randomValues[i / 4], targetBuffer.getInt(i));
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, novaManager, null);
        });
        valueOperator.lockWrite(s);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(8, randomValues[0]);
        putInt(12, randomValues[1]);
        putInt(16, randomValues[2]);
        valueOperator.unlockWrite(s);
        putter.join();
    }

    @Test
    public void cannotPutInDeletedValueTest() {
        valueOperator.deleteValue(s);
        assertEquals(FALSE, valueOperator.put(null, ctx, null, null, novaManager, null));
    }

    @Test
    public void cannotPutToValueOfDifferentVersionTest() {
        s.setAllocVersion(2);
        assertEquals(RETRY, valueOperator.put(null, ctx, null, null, novaManager, null));
    }

    @Test
    public void computeTest() {
        int value = new Random().nextInt(128);
        putInt(8, value);
        valueOperator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);
        });
        assertEquals(value * 2, getInt(8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeUpperBoundTest() {
        valueOperator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(12, 10);
        });
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeLowerBoundTest() {
        valueOperator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(-1, 10);
        });
    }

    @Test
    public void cannotComputeReadLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0]);
        putInt(12, randomValues[1]);
        putInt(16, randomValues[2]);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            valueOperator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            });
        });
        valueOperator.lockRead(s);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int[] results = new int[3];
        for (int i = 0; i < 3; i++) {
            results[i] = getInt(i * 4 + 8);
        }
        valueOperator.unlockRead(s);
        computer.join();
        assertArrayEquals(randomValues, results);
    }

    @Test
    public void cannotComputeWriteLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(8, randomValues[0] - 1);
        putInt(12, randomValues[1] - 1);
        putInt(16, randomValues[2] - 1);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            valueOperator.compute(s, oakWBuffer -> {
                for (int i = 0; i < 12; i += 4) {
                    oakWBuffer.putInt(i, oakWBuffer.getInt(i) + 1);
                }
            });
        });
        valueOperator.lockWrite(s);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        for (int i = 8; i < 20; i += 4) {
            putInt(i, getInt(i) + 1);
        }
        valueOperator.unlockWrite(s);
        computer.join();
        assertNotEquals(randomValues[0], getInt(8));
        assertNotEquals(randomValues[1], getInt(12));
        assertNotEquals(randomValues[2], getInt(16));
    }

    @Test
    public void cannotComputeDeletedValueTest() {
        valueOperator.deleteValue(s);
        assertEquals(FALSE, valueOperator.compute(s, oakWBuffer -> {
        }));
    }

    @Test
    public void cannotComputeValueOfDifferentVersionTest() {
        s.setAllocVersion(2);
        assertEquals(RETRY, valueOperator.compute(s, oakWBuffer -> {
        }));
    }
}
