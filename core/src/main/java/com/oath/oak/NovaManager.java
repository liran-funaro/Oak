package com.oath.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private DisposableThreadLocal<List<Slice>> releaseLists = new DisposableThreadLocal<List<Slice>>() {
        @Override
        List<Slice> initObject(long tid) {
            return new ArrayList<>(RELEASE_LIST_LIMIT);
        }

        @Override
        void dispose(List<Slice>[] oldObjects) {
            for (List<Slice> o : oldObjects) {
                if (o != null) {
                    releaseList(o);
                }
            }
        }
    };
    private AtomicInteger globalNovaNumber;
    private OakBlockMemoryAllocator allocator;

    NovaManager(OakBlockMemoryAllocator allocator) {
        globalNovaNumber = new AtomicInteger(1);
        this.allocator = allocator;
    }

    @Override
    public void close() {
        allocator.close();
    }

    @Override
    public boolean isClosed() {
        return allocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    @Override
    public void allocate(Slice s, int size, Allocate allocate) {
        boolean allocated = allocator.allocate(s, size, allocate);
        assert allocated;
        s.setVersion(globalNovaNumber.get());
    }

    @Override
    public void release(Slice s) {
        List<Slice> curReleaseList = releaseLists.get();
        curReleaseList.add(new Slice(s));
        if (curReleaseList.size() >= RELEASE_LIST_LIMIT) {
            releaseList(curReleaseList);
        }
    }

    void releaseList(List<Slice> slices) {
        globalNovaNumber.incrementAndGet();
        for (Slice allocToRelease : slices) {
            allocator.free(allocToRelease);
        }
        slices.clear();
    }

    @Override
    public void readByteBuffer(Slice s) {
        allocator.readByteBuffer(s);
    }
}
