package com.oath.oak;

public abstract class DisposableThreadLocal<T> {

    class FixedDisposableThreadLocal {
        final ThreadIndexCalculator calculator;
        final T[] threadLocal;

        @SuppressWarnings("unchecked")
        FixedDisposableThreadLocal(int initSize) {
            calculator = new ThreadIndexCalculator(initSize);
            threadLocal = (T[]) new Object[initSize];
        }

        int size() {
            return calculator.size();
        }

        T get(long tid) throws ThreadIndexCalculator.IndexFullException {
            int threadIndex = calculator.getIndex(tid);
            T ret = threadLocal[threadIndex];
            if (ret == null) {
                ret = initObject(tid);
                threadLocal[threadIndex] = ret;
            }
            return ret;
        }
    }

    private FixedDisposableThreadLocal internal;

    public DisposableThreadLocal() {
        internal = new FixedDisposableThreadLocal(ThreadIndexCalculator.MAX_THREADS);
    }

    public DisposableThreadLocal(int initSize) {
        internal = new FixedDisposableThreadLocal(initSize);
    }

    public int size() {
        return internal.size();
    }

    abstract T initObject(long tid);

    int getNewSize(int oldSize) {
        return  oldSize * 2;
    }

    void dispose(T[] oldObjects) {
    }

    synchronized public void resize(FixedDisposableThreadLocal oldInternal, int newSize) {
        if (internal != oldInternal) {
            // Another thread did the resize for us
            return;
        }

        reset(newSize);
    }

    public void reset() {
        reset(ThreadIndexCalculator.MAX_THREADS);
    }

    synchronized public void reset(int newSize) {
        FixedDisposableThreadLocal oldInternal = internal;
        internal = new FixedDisposableThreadLocal(newSize);
        dispose(internal.threadLocal);
    }

    public T get(long tid) {
        while (true) {
            FixedDisposableThreadLocal curInternal = internal;
            try {
                return curInternal.get(tid);
            } catch (ThreadIndexCalculator.IndexFullException e) {
                resize(curInternal, getNewSize(curInternal.size()));
            }
        }
    }

    public T get() {
        return get(Thread.currentThread().getId());
    }
}
