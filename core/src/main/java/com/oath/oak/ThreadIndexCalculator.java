
package com.oath.oak;

import sun.misc.Unsafe;

import java.util.NoSuchElementException;

class ThreadIndexCalculator {

    public static class IndexFullException extends Exception {}

    public static final int MAX_THREADS = 64;
    private static final int INVALID_THREAD_ID = -1;
    // Long for correctness and anti false-sharing
    private final long[] indices;

    public ThreadIndexCalculator() {
        this(MAX_THREADS);
    }

    public ThreadIndexCalculator(int size) {
        indices = new long[size];

        for (int i = 0; i < size; ++i) {
            indices[i] = INVALID_THREAD_ID;
        }
    }

    public int size() {
        return indices.length;
    }

    private int calcIndex(long threadID) {
        return (int) (threadID % size());
    }

    private int nextIndex(int index) {
        return (index + 1) % size();
    }

    private int getExistingIndex(long threadID) throws IndexFullException {
        final int threadMainIndex = calcIndex(threadID);
        int currentIndex = threadMainIndex;

        while (indices[currentIndex] != threadID) {
            if (indices[currentIndex] == INVALID_THREAD_ID) {
                // negative output indicates that a new index need to be created for this thread id
                return -1 * currentIndex;
            }
            currentIndex = nextIndex(currentIndex);
            if (currentIndex == threadMainIndex) {
                throw new IndexFullException();
            }
        }

        return currentIndex;
    }

    private boolean setIndex(long tid, int threadIndex) {
        return UnsafeUtils.unsafe.compareAndSwapLong(indices,
                Unsafe.ARRAY_LONG_BASE_OFFSET + threadIndex * Unsafe.ARRAY_LONG_INDEX_SCALE,
                INVALID_THREAD_ID, tid);
    }

    public int getIndex() throws IndexFullException {
        return getIndex(Thread.currentThread().getId());
    }

    public int getIndex(long tid) throws IndexFullException {
        final int foundThreadIndex = getExistingIndex(tid);
        if (foundThreadIndex > 0 || (foundThreadIndex == 0 && tid == indices[0])) {
            // due to multiplying by -1 check this special array element
            return foundThreadIndex;
        }

        int currentIndex = Math.abs(foundThreadIndex);

        while (!setIndex(tid, currentIndex)) {
            currentIndex = nextIndex(currentIndex);
            if (currentIndex == foundThreadIndex) {
                throw new IndexFullException();
            }
        }

        return currentIndex;
    }

    public void releaseIndex() {
        releaseIndex(Thread.currentThread().getId());
    }

    public void releaseIndex(long tid) {
        int index;

        try {
            index = getExistingIndex(tid);
        } catch (IndexFullException e) {
            throw new NoSuchElementException();
        }
        if (index < 0) {
            // There is no such thread index in the calculator, so throw NoSuchElementException
            // Probably releasing the same thread twice
            throw new NoSuchElementException();
        }
        indices[index] = INVALID_THREAD_ID;
    }

    public static ThreadIndexCalculator newInstance() {
        return new ThreadIndexCalculator();
    }
}
