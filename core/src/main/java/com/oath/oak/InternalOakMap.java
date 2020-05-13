/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.*;
import static com.oath.oak.ValueUtils.ValueResult.*;

class InternalOakMap<K, V> {

    /*-------------- Members --------------*/

    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final OakComparator<K> comparator;
    private final MemoryManager memoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    // The reference count is used to count the upper objects wrapping this internal map:
    // OakMaps (including subMaps and Views) when all of the above are closed,
    // his map can be closed and memory released.
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final ValueUtils valueOperator;
    final static int MAX_RETRIES = 1024;

    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */

    InternalOakMap(K minKey, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
                   OakComparator<K> oakComparator, MemoryManager memoryManager, int chunkMaxItems,
                   ValueUtils valueOperator) {

        this.size = new AtomicInteger(0);
        this.memoryManager = memoryManager;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.comparator = oakComparator;

        this.minKey = ByteBuffer.allocateDirect(this.keySerializer.calculateSize(minKey));
        // newly allocated buffer is always positioned at its beginning.
        this.keySerializer.serialize(minKey, this.minKey);

        // This is a trick for letting us search through the skiplist using both serialized and unserialized keys.
        // Might be nicer to replace it with a proper visitor
        Comparator<Object> mixedKeyComparator = (o1, o2) -> {
            if (o1 instanceof ByteBuffer) {
                if (o2 instanceof ByteBuffer) {
                    return oakComparator.compareSerializedKeys((ByteBuffer) o1, (ByteBuffer) o2);
                } else {
                    // Note the inversion of arguments, hence sign flip
                    return (-1) * oakComparator.compareKeyAndSerializedKey((K) o2, (ByteBuffer) o1);
                }
            } else {
                if (o2 instanceof ByteBuffer) {
                    return oakComparator.compareKeyAndSerializedKey((K) o1, (ByteBuffer) o2);
                } else {
                    return oakComparator.compareKeys((K) o1, (K) o2);
                }
            }
        };
        this.skiplist = new ConcurrentSkipListMap<>(mixedKeyComparator);

        Chunk<K, V> head = new Chunk<>(this.minKey, null, this.comparator, memoryManager, chunkMaxItems,
                this.size, keySerializer, valueSerializer, valueOperator);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);
        this.valueOperator = valueOperator;
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    void close() {
        int res = referenceCount.decrementAndGet();
        // once reference count is zeroed, the map meant to be deleted and should not be used.
        // reference count will never grow again
        if (res == 0) {
            try {
                memoryManager.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // yet another object started to refer to this internal map
    void open() {
        while (true) {
            int res = referenceCount.get();
            // once reference count is zeroed, the map meant to be deleted and should not be used.
            // reference count should never grow again and the referral is not allowed
            if (res == 0) {
                throw new ConcurrentModificationException();
            }
            // although it is costly CAS is used here on purpose so we never increase
            // zeroed reference count
            if (referenceCount.compareAndSet(res, res + 1)) {
                break;
            }
        }
    }

    /*-------------- size --------------*/

    /**
     * @return current off heap memory usage in bytes
     */
    long memorySize() {
        return memoryManager.allocated();
    }

    int entries() {
        return size.get();
    }

    /*-------------- Context --------------*/

    // This array holds for each thread a context instance to avoid redundant instantiation and GC.
    private final DisposableThreadLocal<ThreadContext> threadLocalContext = new DisposableThreadLocal<ThreadContext>() {
        @Override
        ThreadContext initObject(long tid) {
            return new ThreadContext((int) tid, valueOperator);
        }
    };

    /**
     * Should only be called from API methods at the beginning of the method and be reused in internal calls.
     * @return a thread-local context instance.
     */
    ThreadContext getThreadLocalContext() {
        ThreadContext ret = threadLocalContext.get();
        ret.invalidate();
        return ret;
    }

    /*-------------- Methods --------------*/

    /**
     * finds and returns the chunk where key should be located, starting from given chunk
     */
    private Chunk<K, V> iterateChunks(Chunk<K, V> c, K key) {
        // find chunk following given chunk (next)
        Chunk<K, V> next = c.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compareKeyAndSerializedKey(key, next.minKey) >= 0)) {
            c = next;
            next = c.next.getReference();
        }

        return c;
    }

    boolean overwriteExistingValueForMove(ThreadContext ctx, V newVal, Chunk<K, V> c) {
        // given old entry index (inside ctx) and new value, while old value is locked,
        // allocate new value, new value is going to be locked as well, write the new value
        c.writeValue(ctx, newVal, true);

        // in order to connect/overwrite the old entry to point to new value
        // we need to publish as in the normal write process
        if (!c.publish()) {
            c.releaseNewValue(ctx);
            rebalance(c);
            return false;
        }

        // updating the old entry index
        if (c.linkValue(ctx) != TRUE) {
            c.releaseNewValue(ctx);
            c.unpublish();
            return false;
        }

        c.unpublish();
        checkRebalance(c);
        return true;
    }

    /**
     * @param c - Chunk to rebalance
     */
    private void rebalance(Chunk<K, V> c) {

        if (c == null) {
            return;
        }
        Rebalancer<K, V> rebalancer = new Rebalancer<>(c, memoryManager, keySerializer,
                valueSerializer, valueOperator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        ThreadContext ctx = getThreadLocalContext();
        rebalancer.createNewChunks(ctx); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        engaged.forEach(Chunk::release);
    }

    private void checkRebalance(Chunk<K, V> c) {
        if (c.shouldRebalance()) {
            rebalance(c);
        }
    }

    private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

        updateLastChild(engaged, children);
        int countIterations = 0;
        Chunk<K, V> firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous chunk to our chunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            countIterations++;
            assert (countIterations < 10000); // this loop is not supposed to be infinite

            // start with first chunk (i.e., head)
            Map.Entry<Object, Chunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

            Chunk<K, V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
            Chunk<K, V> curr = (prev != null) ? prev.next.getReference() : null;

            // if didn't succeed to find prev through the skiplist - start from the head
            if (prev == null || curr != firstEngaged) {
                prev = null;
                curr = skiplist.firstEntry().getValue();    // TODO we can store&update head for a little efficiency
                // iterate until found chunk or reached end of list
                while ((curr != firstEngaged) && (curr != null)) {
                    prev = curr;
                    curr = curr.next.getReference();
                }
            }

            // chunk is head, we need to "add it to the list" for linearization point
            if (curr == firstEngaged && prev == null) {
                this.head.compareAndSet(firstEngaged, children.get(0));
                break;
            }
            // chunk is not in list (someone else already updated list), so we're done with this part
            if (curr == null) {
                break;
            }

            // if prev chunk is marked - it is deleted, need to help split it and then continue
            if (prev.next.isMarked()) {
                rebalance(prev);
                continue;
            }

            // try to CAS prev chunk's next - from chunk (that we split) into c1
            // c1 is the old chunk's replacement, and is already connected to c2
            // c2 is already connected to old chunk's next - so all we need to do is this replacement
            if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
                    (!prev.next.isMarked())) {
                // if we're successful, or we failed but prev is not marked - so it means someone else was successful
                // then we're done with loop
                break;
            }
        }

    }

    private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
        Chunk<K, V> lastEngaged = engaged.get(engaged.size() - 1);
        Chunk<K, V> nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged chunk as deleted
        Chunk<K, V> lastChild = children.get(children.size() - 1);

        lastChild.next.compareAndSet(null, nextToLast, false, false);
    }

    private void updateIndexAndNormalize(List<Chunk<K, V>> engagedChunks, List<Chunk<K, V>> children) {
        Iterator<Chunk<K, V>> iterEngaged = engagedChunks.iterator();
        Iterator<Chunk<K, V>> iterChildren = children.iterator();

        Chunk<K, V> firstEngaged = iterEngaged.next();
        Chunk<K, V> firstChild = iterChildren.next();

        // need to make the new chunks available, before removing old chunks
        skiplist.replace(firstEngaged.minKey, firstEngaged, firstChild);

        // remove all old chunks from index.
        while (iterEngaged.hasNext()) {
            Chunk engagedToRemove = iterEngaged.next();
            skiplist.remove(engagedToRemove.minKey, engagedToRemove); // conditional remove is used
        }

        // now after removing old chunks we can start normalizing
        firstChild.normalize();

        // for simplicity -  naive lock implementation
        // can be implemented without locks using versions on next pointer in skiplist
        while (iterChildren.hasNext()) {
            Chunk<K, V> childToAdd;
            synchronized (childToAdd = iterChildren.next()) {
                if (childToAdd.state() == Chunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }
    }

    private boolean inTheMiddleOfRebalance(Chunk<K, V> c) {
        State state = c.state();
        if (state == State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            return true;
        }
        if (state == State.FROZEN || state == State.RELEASED) {
            rebalance(c);
            return true;
        }
        return false;
    }

    private boolean finalizeDeletion(Chunk<K, V> c, ThreadContext ctx) {
        if (ctx.isKeyValid()) {
            if (c.finalizeDeletion(ctx)) {
                rebalance(c);
                return true;
            }
        }
        return false;
    }

    /**
     * This function completes the insertion of the value reference to the entry
     * (reflected in {@code ctx}), and updates the value's version in {@code ctx} IF NEEDED.
     * In case, the linking cannot be done (i.e., a concurrent rebalance), than
     * rebalance is called.
     *
     * @param c      - the chuck pointed by {@code ctx}
     * @param ctx - holds the value reference, old version, and relevant entry to update
     * @return whether the caller method should restart (if a rebalance was executed).
     */
    private boolean updateVersionAfterLinking(Chunk<K, V> c, ThreadContext ctx) {
        if (c.completeLinking(ctx) == EntrySet.INVALID_VERSION) {
            rebalance(c);
            return true;
        }
        return false;
    }

    /*-------------- OakMap Methods --------------*/

    static void negateVersion(Slice s) {
        s.setVersion(-Math.abs(s.getVersion()));
    }

    V put(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            // If there is a matching value reference for the given key, and it is not marked as deleted, then this put
            // changes the slice pointed by this value reference.
            if (ctx.isValueValid()) {
                if (updateVersionAfterLinking(c, ctx)) {
                    continue;
                }
                Result res = valueOperator.exchange(c, ctx, value, transformer, valueSerializer, memoryManager,
                    this);
                if (res.operationResult == TRUE) {
                    return (V) res.value;
                }
                // Exchange failed because the value was deleted/moved between lookup and exchange. Continue with
                // insertion.
                continue;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, ctx)) {
                continue;
            }
            /* If the key was not found, there is no entry with that key in Oak, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted, marked just off-heap). After calling {@code deleteValueFinish(Chunk<K, V>, ThreadContext)},
            the version of this entry should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in ctx (this way, if the version is already negative, it
            remains negative).
             */
            if (ctx.isKeyValid()) {
                negateVersion(ctx.value);
                assert ctx.entryIndex > 0;
            } else {
                // There was no such key found, while EntrySet allocates the entry
                // (holding the key) ctx is going to be updated to be used by EntrySet's
                // subsequent requests to write value
                boolean isAllocationSuccessful = c.allocateEntryAndKey(ctx, key);
                if (!isAllocationSuccessful) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ctx, key);
                if (prevEi != ctx.entryIndex) {
                    // our entry wasn't inserted because other entry with same key was found.
                    // If this entry (with index prevEi) is valid we should move to continue
                    // with existing entry scenario, otherwise we can reuse this entry because
                    // its value is invalid.
                    c.releaseKey(ctx);
                    if (!c.isValueRefValid(prevEi)) {
                        continue;
                    }
                    // We use an existing entry only if its value reference is invalid
                    ctx.entryIndex = prevEi;
                }
            }

            c.writeValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null; // null can be returned only in zero-copy case
            }
        }

        throw new RuntimeException("put failed: reached retry limit (1024).");
    }

    Result putIfAbsent(K key, V value, OakTransformer<V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);

            if (ctx.isValueValid()) {
                if (updateVersionAfterLinking(c, ctx)) {
                    continue;
                }
                if (transformer == null) {
                    return ctx.result.withFlag(FALSE);
                }
                Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
                if (res.operationResult == TRUE) {
                    return res;
                }
                continue;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, ctx)) {
                continue;
            }
            /* If the key was not found, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted). After calling {@code deleteValueFinish(Chunk<K, V>, ThreadContext)}, the version of this
            entry should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in ctx (this way, if the version is already negative, it
            remains negative).
             */
            if (ctx.isKeyValid()) {
                // There's an entry for this key, but it isn't linked to any value (in which case valueReference is
                // DELETED_VALUE)
                // or it's linked to a deleted value that is referenced by valueReference (a valid one)
                negateVersion(ctx.value);
                assert ctx.entryIndex > 0;
            } else {
                // There was no such key found, while EntrySet allocates the entry
                // (holding the key) ctx is going to be updated to be used by EntrySet's
                // subsequent requests to write value
                boolean isAllocationSuccessful = c.allocateEntryAndKey(ctx, key);
                if (!isAllocationSuccessful) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ctx, key);
                if (prevEi != ctx.entryIndex) {
                    // our entry wasn't inserted because other entry with same key was found.
                    // If this entry (with index prevEi) is valid we should return false,
                    // otherwise we can reuse this entry because its value is invalid.
                    c.releaseKey(ctx);
                    // for non-zc interface putIfAbsent returns the previous value associated with
                    // the specified key, or null if there was no mapping for the key.
                    // so we need to create a slice to let transformer create the value object
                    boolean isAllocated = c.readValueFromEntryIndex(ctx.tempValue, prevEi);
                    if (isAllocated) {
                        if (transformer == null) {
                            return ctx.result.withFlag(FALSE);
                        }
                        Result res = valueOperator.transform(ctx.result, ctx.tempValue, transformer);
                        if (res.operationResult == TRUE) {
                            return res;
                        }
                        continue;
                    } else {
                        // both threads compete for the put
                        ctx.entryIndex = prevEi;
                    }
                }
            }

            c.writeValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return ctx.result.withFlag(TRUE);
            }
        }

        throw new RuntimeException("putIfAbsent failed: reached retry limit (1024).");
    }

    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWriteBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            if (ctx.isValueValid()) {
                if (updateVersionAfterLinking(c, ctx)) {
                    continue;
                }
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already found as deleted, continue to allocate a new value slice
                    return false;
                } else if (res == RETRY) {
                    continue;
                }
            }

            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, ctx)) {
                continue;
            }
            /* If there is no entry with that key in Oak, the previous version
            associated with the entry is INVALID_VERSION.
            Otherwise, there is an entry with the given key and its value is deleted (or at least in the process of
            being deleted). After calling {@code deleteValueFinish(Chunk<K, V>, ThreadContext)}, the version of this
            entry should be negative.
            Concurrent insertions, however, may cause the version to become valid again.
            Thus, to ensure that the operation behaves as if it itself unlinked the deleted value, we take the minus
            the absolute value of the version written in ctx (this way, if the version is already negative, it
            remains negative).
             */

            // we come here when no key was found, which can be in 3 cases:
            // 1. no entry in the linked list at all
            // 2. entry in the linked list, but the value reference is INVALID_VALUE_REFERENCE
            // 3. entry in the linked list, the value referenced is marked as deleted
            if (ctx.isKeyValid()) {
                negateVersion(ctx.value);
                assert ctx.entryIndex > 0;
            } else {
                // There was no such key found, while EntrySet allocates the entry
                // (holding the key) ctx is going to be updated to be used by EntrySet's
                // subsequent requests to write value
                boolean isAllocationSuccessful = c.allocateEntryAndKey(ctx, key);
                if (!isAllocationSuccessful) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ctx, key);
                if (prevEi != ctx.entryIndex) {
                    // our entry wasn't inserted because other entry with same key was found.
                    // If this entry (with index prevEi) is valid we should move to continue
                    // with existing entry scenario (compute), otherwise we can reuse this entry because
                    // its value is invalid.
                    c.releaseKey(ctx);
                    if (c.isValueRefValid(prevEi)) {
                        continue;
                    } else {
                        ctx.entryIndex = prevEi;
                    }
                }
            }

            c.writeValue(ctx, value, false); // write value in place

            if (!c.publish()) {
                c.releaseNewValue(ctx);
                rebalance(c);
                continue;
            }

            if (c.linkValue(ctx) != TRUE) {
                c.releaseNewValue(ctx);
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }

        throw new RuntimeException("putIfAbsentComputeIfPresent failed: reached retry limit (1024).");
    }

    Result remove(K key, V oldValue, OakTransformer<V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        // when logicallyDeleted is true, it means we have marked the value as deleted.
        // Note that the entry will remain linked until rebalance happens.
        boolean logicallyDeleted = false;
        V v = null;

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);

            if (!ctx.isKeyValid()) {
                // There is no such key. If we did logical deletion and someone else did the physical deletion,
                // then the old value is saved in v. Otherwise v is (correctly) null
                return transformer == null ? ctx.result.withFlag(logicallyDeleted ? TRUE : FALSE) : ctx.result.withValue(v);
            } else if (!ctx.isValueValid()) {
                if (!c.finalizeDeletion(ctx)) {
                    return transformer == null ? ctx.result.withFlag(logicallyDeleted ? TRUE : FALSE) : ctx.result.withValue(v);
                }
                rebalance(c);
                continue;
            }

            if (inTheMiddleOfRebalance(c) || updateVersionAfterLinking(c, ctx)) {
                continue;
            }

            if (logicallyDeleted) {
                // This is the case where we logically deleted this entry (marked the value as deleted), but someone
                // reused the entry before we unlinked it. We have the previous value saved in v.
                return transformer == null ? ctx.result.withFlag(TRUE) : ctx.result.withValue(v);
            } else {
                Result removeResult = valueOperator.remove(ctx, memoryManager,
                        oldValue, transformer);
                if (removeResult.operationResult == FALSE) {
                    // we didn't succeed to remove the value: it didn't contain oldValue, or was already marked
                    // as deleted by someone else)
                    return ctx.result.withFlag(FALSE);
                } else if (removeResult.operationResult == RETRY) {
                    continue;
                }
                // we have marked this value as deleted (successful remove)
                logicallyDeleted = true;
                v = (V) removeResult.value;
            }

            assert ctx.entryIndex > 0;
            assert ctx.value.isAllocated();

            ctx.valueState = EntrySet.ValueState.DELETED_NOT_FINALIZED;
            // publish
            if (c.finalizeDeletion(ctx)) {
                rebalance(c);
            }
        }

        throw new RuntimeException("remove failed: reached retry limit (1024).");
    }

    OakDetachedBuffer get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }
            if (updateVersionAfterLinking(c, ctx)) {
                continue;
            }

            return getValueDetachedBuffer(ctx);
        }

        throw new RuntimeException("get failed: reached retry limit (1024).");
    }

    boolean computeIfPresent(K key, Consumer<OakWriteBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);

            if (ctx.isValueValid()) {
                if (updateVersionAfterLinking(c, ctx)) {
                    continue;
                }
                ValueUtils.ValueResult res = valueOperator.compute(ctx.value, computer);
                if (res == TRUE) {
                    // compute was successful and the value wasn't found deleted; in case
                    // this value was already marked as deleted, continue to construct another slice
                    return true;
                } else if (res == RETRY) {
                    continue;
                }
            }
            return false;
        }

        throw new RuntimeException("computeIfPresent failed: reached retry limit (1024).");
    }

    /**
     * Used when value of a key was possibly moved and we try to search for the given key
     * through the OakMap again.
     *
     * @param ctx The context key should be initialized with the key to refresh, and the context value
     *            will be updated with the refreshed value.
     * @reutrn    true if the refresh was successful.
     */
    boolean refreshValuePosition(ThreadContext ctx) {
        K deserializedKey = keySerializer.deserialize(ctx.key.getDataByteBuffer());

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(deserializedKey); // find chunk matching key
            c.lookUp(ctx, deserializedKey);
            if (!ctx.isValueValid()) {
                return false;
            }

            if (updateVersionAfterLinking(c, ctx)) {
                continue;
            }

            return true;
        }

        throw new RuntimeException("refreshValuePosition failed: reached retry limit (1024).");
    }

    /**
     * See {@code refreshValuePosition(ctx)} for more details.
     *
     * @param keySlice   the key to refresh
     * @param valueSlice the output value to update
     * @return           true if the refresh was successful.
     */
    boolean refreshValuePosition(Slice keySlice, Slice valueSlice) {
        ThreadContext ctx = getThreadLocalContext();
        ctx.key.copyFrom(keySlice);
        boolean isSuccessful = refreshValuePosition(ctx);

        if (!isSuccessful) {
            return false;
        }

        valueSlice.copyFrom(ctx.value);
        return true;
    }

    private <T> T getValueTransformation(ByteBuffer key, OakTransformer<T> transformer) {
        K deserializedKey = keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    <T> T getValueTransformation(K key, OakTransformer<T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            if (updateVersionAfterLinking(c, ctx)) {
                continue;
            }
            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            if (res.operationResult == RETRY) {
                continue;
            }
            return (T) res.value;
        }

        throw new RuntimeException("getValueTransformation failed: reached retry limit (1024).");
    }

    <T> T getKeyTransformation(K key, OakTransformer<T> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        ThreadContext ctx = getThreadLocalContext();
        Chunk<K, V> c = findChunk(key);
        c.lookUp(ctx, key);
        if (!ctx.isValueValid()) {
            return null;
        }
        return transformer.apply(ctx.key.getDuplicatedReadByteBuffer().slice());
    }

    OakDetachedBuffer getMinKey() {
        Chunk<K, V> c = skiplist.firstEntry().getValue();
        ThreadContext ctx = getThreadLocalContext();
        boolean isAllocated = c.readMinKey(ctx.key);
        return isAllocated ? getKeyDetachedBuffer(ctx) : null;
    }

    <T> T getMinKeyTransformation(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.firstEntry().getValue();
        ThreadContext ctx = getThreadLocalContext();
        boolean isAllocated = c.readMinKey(ctx.tempKey);
        return isAllocated ? transformer.apply(ctx.tempKey.getDataByteBuffer()) : null;
    }

    OakDetachedBuffer getMaxKey() {
        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        ThreadContext ctx = getThreadLocalContext();
        boolean isAllocated = c.readMaxKey(ctx.key);
        return isAllocated ? getKeyDetachedBuffer(ctx) : null;
    }

    <T> T getMaxKeyTransformation(OakTransformer<T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }
        ThreadContext ctx = getThreadLocalContext();
        boolean isAllocated = c.readMaxKey(ctx.tempKey);
        return isAllocated ? transformer.apply(ctx.tempKey.getDataByteBuffer()) : null;
    }

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk<K, V> findChunk(K key) {
        Chunk<K, V> c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    V replace(K key, V value, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return null;
            }

            // will return null if the value is deleted
            Result result = valueOperator.exchange(c, ctx, value, valueDeserializeTransformer, valueSerializer,
                    memoryManager, this);
            if (result.operationResult != RETRY) {
                return (V) result.value;
            }
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    boolean replace(K key, V oldValue, V newValue, OakTransformer<V> valueDeserializeTransformer) {
        ThreadContext ctx = getThreadLocalContext();

        for (int i = 0; i < MAX_RETRIES; i++) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            c.lookUp(ctx, key);
            if (!ctx.isValueValid()) {
                return false;
            }

            ValueUtils.ValueResult res = valueOperator.compareExchange(c, ctx, oldValue, newValue,
                    valueDeserializeTransformer, valueSerializer, memoryManager, this);
            if (res == RETRY) {
                continue;
            }
            return res == TRUE;
        }

        throw new RuntimeException("replace failed: reached retry limit (1024).");
    }

    Map.Entry<K, V> lowerEntry(K key) {
        Map.Entry<Object, Chunk<K, V>> lowerChunkEntry = skiplist.lowerEntry(key);
        if (lowerChunkEntry == null) {
            /* we were looking for the minimal key */
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        ThreadContext ctx = getThreadLocalContext();

        Chunk<K, V> c = lowerChunkEntry.getValue();
        /* Iterate chunk to find prev(key) */
        Chunk.AscendingIter chunkIter = c.ascendingIter();
        int prevIndex = chunkIter.next(ctx);

        while (chunkIter.hasNext()) {
            int nextIndex = chunkIter.next(ctx);
            c.readKeyFromEntryIndex(ctx.tempKey, nextIndex);
            int cmp = comparator.compareKeyAndSerializedKey(key, ctx.tempKey.getDataByteBuffer());
            if (cmp <= 0) {
                break;
            }
            prevIndex = nextIndex;
        }

        /* Edge case: we're looking for the lowest key in the map and it's still greater than minkey
            (in which  case prevKey == key) */
        c.readKeyFromEntryIndex(ctx.tempKey, prevIndex);
        ByteBuffer buff = ctx.tempKey.getDataByteBuffer();
        if (comparator.compareKeyAndSerializedKey(key, buff) == 0) {
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }
        K keyDeserialized = keySerializer.deserialize(buff.slice());

        // get value associated with this (prev) key
        boolean isAllocated = c.readValueFromEntryIndex(ctx.value, prevIndex);
        if (!isAllocated){ // value reference was invalid, try again
            return lowerEntry(key);
        }
        Result valueDeserialized = valueOperator.transform(ctx.result, ctx.value,
                valueSerializer::deserialize);
        if (valueDeserialized.operationResult != TRUE) {
            return lowerEntry(key);
        }
        return new AbstractMap.SimpleImmutableEntry<>(keyDeserialized, (V) valueDeserialized.value);
    }

    /*-------------- Iterators --------------*/

    private OakDetachedReadBuffer getKeyDetachedBuffer(ThreadContext ctx) {
        return new OakDetachedReadBuffer<>(new KeyBuffer(ctx.key));
    }

    private OakDetachedReadValueBufferSynced getValueDetachedBuffer(ThreadContext ctx) {
        return new OakDetachedReadValueBufferSynced(ctx.key, ctx.value, valueOperator, InternalOakMap.this);
    }

    private static class IteratorState<K, V> {

        private Chunk<K, V> chunk;
        private Chunk.ChunkIter chunkIter;
        private int index;

        public void set(Chunk<K, V> chunk, Chunk.ChunkIter chunkIter, int index) {
            this.chunk = chunk;
            this.chunkIter = chunkIter;
            this.index = index;
        }

        private IteratorState(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter, int nextIndex) {
            this.chunk = nextChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        Chunk<K, V> getChunk() {
            return chunk;
        }

        Chunk.ChunkIter getChunkIter() {
            return chunkIter;
        }

        public int getIndex() {
            return index;
        }

        static <K, V> IteratorState<K, V> newInstance(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter) {
            return new IteratorState<>(nextChunk, nextChunkIter, NONE_NEXT);
        }

    }

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements Iterator<T> {

        private K lo;

        /**
         * upper bound key, or null if to end
         */
        private K hi;
        /**
         * inclusion flag for lo
         */
        private boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * the next node to return from next();
         */
        private IteratorState<K, V> state;

        /**
         * An iterator cannot be accesses concurrently by multiple threads.
         * Thus, it is safe to have its own thread context.
         */
        protected ThreadContext ctx;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            if (lo != null && hi != null && comparator.compare(lo, hi) > 0) {
                throw new IllegalArgumentException("inconsistent range");
            }

            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.isDescending = isDescending;
            this.ctx = new ThreadContext(-1, valueOperator);
            initState(isDescending, lo, loInclusive, hi, hiInclusive);

        }

        boolean tooLow(ByteBuffer key) {
            int c;
            return (lo != null && ((c = comparator.compareKeyAndSerializedKey(lo, key)) > 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(ByteBuffer key) {
            int c;
            return (hi != null && ((c = comparator.compareKeyAndSerializedKey(hi, key)) < 0 ||
                    (c == 0 && !hiInclusive)));
        }


        boolean inBounds(ByteBuffer key) {
            if (!isDescending) {
                return !tooHigh(key);
            } else {
                return !tooLow(key);
            }
        }

        public final boolean hasNext() {
            return (state != null);
        }

        private void initAfterRebalance() {
            //TODO - refactor to use ByeBuffer without deserializing.
            state.getChunk().readKeyFromEntryIndex(ctx.tempKey, state.getIndex());
            K nextKey = keySerializer.deserialize(ctx.tempKey.getDataByteBuffer().slice());

            if (isDescending) {
                hiInclusive = true;
                hi = nextKey;
            } else {
                loInclusive = true;
                lo = nextKey;
            }

            // Update the state to point to last returned key.
            initState(isDescending, lo, loInclusive, hi, hiInclusive);

            if (state == null) {
                // There are no more elements in Oak after nextKey, so throw NoSuchElementException
                throw new NoSuchElementException();
            }
        }


        // the actual next()
        abstract public T next();

        /**
         * Advances next to higher entry.
         * Return previous index
         *
         * @return The first long is the key's reference, the integer is the value's version and the second long is
         * the value's reference. If {@code needsValue == false}, then the value of the map entry is {@code null}.
         */
        void advance(boolean needsValue) {
            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            // build the entry context that sets key references and does not check for value validity.
            state.getChunk().readKeyFromEntryIndex(ctx, state.getIndex());
            assert ctx.isKeyValid();

            if (needsValue) {
                // Set value references and checks for value validity.
                // if value is deleted ctx.value is going to be invalid
                state.getChunk().readValueFromEntryIndex(ctx);
                if (ctx.value.isAllocated()) {
                    ctx.value.setVersion(state.getChunk().completeLinking(ctx));
                    // The CAS could not complete due to concurrent rebalance, so rebalance and try again
                    if (!ctx.value.isValidVersion()) {
                        rebalance(state.getChunk());
                        advance(true);
                        return;
                    }
                    // If the value is deleted, advance to the next value
                    if (!ctx.isValueValid()) {
                        advanceState();
                        advance(true);
                        return;
                    }
                } else {
                    advanceState();
                    advance(true);
                    return;
                }
            }
            advanceState();
        }

        /**
         * Advances next to the next entry without creating a ByteBuffer for the key.
         * Return previous index
         */
        void advanceStream(OakDetachedReadBuffer<KeyBuffer> key, OakDetachedReadBuffer<ValueBuffer> value) {
            assert key != null || value != null;
            if (state == null) {
                throw new NoSuchElementException();
            }
            final Chunk c = state.getChunk();
            final int curIndex = state.getIndex();
            final Chunk.State chunkState = c.state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            if (key != null) {
                c.readKeyFromEntryIndex(key.buffer, curIndex);
            }

            if (value != null) {
                if (!state.getChunk().readValueFromEntryIndex(value.buffer, curIndex)) {
                    // If the current value is deleted, then advance and try again
                    advanceState();
                    advanceStream(key, value);
                    return;
                }
            }

            advanceState();
        }

        private void initState(boolean isDescending, K lowerBound, boolean lowerInclusive,
                               K upperBound, boolean upperInclusive) {

            Chunk.ChunkIter nextChunkIter;
            Chunk<K, V> nextChunk;

            if (!isDescending) {
                if (lowerBound != null) {
                    nextChunk = skiplist.floorEntry(lowerBound).getValue();
                } else {
                    nextChunk = skiplist.floorEntry(minKey).getValue();
                }
                if (nextChunk != null) {
                    nextChunkIter = lowerBound != null ?
                            nextChunk.ascendingIter(ctx, lowerBound, lowerInclusive) : nextChunk.ascendingIter();
                } else {
                    state = null;
                    return;
                }
            } else {
                nextChunk = upperBound != null ? skiplist.floorEntry(upperBound).getValue()
                        : skiplist.lastEntry().getValue();
                if (nextChunk != null) {
                    nextChunkIter = upperBound != null ?
                            nextChunk.descendingIter(ctx, upperBound, upperInclusive) : nextChunk.descendingIter(ctx);
                } else {
                    state = null;
                    return;
                }
            }

            //Init state, not valid yet, must move forward
            state = IteratorState.newInstance(nextChunk, nextChunkIter);
            advanceState();
        }

        private Chunk<K, V> getNextChunk(Chunk<K, V> current) {
            if (!isDescending) {
                return current.next.getReference();
            } else {
                ByteBuffer serializedMinKey = current.minKey;
                Map.Entry<Object, Chunk<K, V>> entry = skiplist.lowerEntry(serializedMinKey);
                if (entry == null) {
                    return null;
                } else {
                    return entry.getValue();
                }
            }
        }

        private Chunk.ChunkIter getChunkIter(Chunk<K, V> current) {
            if (!isDescending) {
                return current.ascendingIter();
            } else {
                return current.descendingIter(ctx);
            }
        }

        private void advanceState() {

            Chunk<K, V> chunk = state.getChunk();
            Chunk.ChunkIter chunkIter = state.getChunkIter();

            while (!chunkIter.hasNext()) { // chunks can have only removed keys
                chunk = getNextChunk(chunk);
                if (chunk == null) {
                    //End of iteration
                    state = null;
                    return;
                }
                chunkIter = getChunkIter(chunk);
            }

            int nextIndex = chunkIter.next(ctx);
            state.set(chunk, chunkIter, nextIndex);

            // The boundary check is costly and need to be performed only when required,
            // meaning not on the full scan.
            // The check of the boundaries under condition is an optimization.
            if ((hi != null && !isDescending) || (lo != null && isDescending)) {
                state.getChunk().readKeyFromEntryIndex(ctx.tempKey, state.getIndex());
                if (!inBounds(ctx.tempKey.getDataByteBuffer())) {
                    state = null;
                }
            }
        }
    }

    class ValueIterator extends Iter<OakDetachedBuffer> {

        private final InternalOakMap<K, V> internalOakMap;

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        @Override
        public OakDetachedBuffer next() {
            advance(true);
            return getValueDetachedBuffer(ctx);
        }
    }

    class ValueStreamIterator extends Iter<OakDetachedBuffer> {

        private final OakDetachedReadBuffer<ValueBuffer> value = new OakDetachedReadBuffer<>(new ValueBuffer(valueOperator));

        ValueStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakDetachedBuffer next() {
            advanceStream(null, value);
            return value;
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        final OakTransformer<T> transformer;

        ValueTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               OakTransformer<T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            Result res = valueOperator.transform(ctx.result, ctx.value, transformer);
            // If this value is deleted, try the next one
            if (res.operationResult == FALSE) {
                return next();
            }
            // if the value was moved, fetch it from the
            else if (res.operationResult == RETRY) {
                T result = getValueTransformation(ctx.key.getDataByteBuffer(), transformer);
                if (result == null) {
                    // the value was deleted, try the next one
                    return next();
                }
                return result;
            }
            return (T) res.value;
        }
    }

    class EntryIterator extends Iter<Map.Entry<OakDetachedBuffer, OakDetachedBuffer>> {

        private final InternalOakMap<K, V> internalOakMap;

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        public Map.Entry<OakDetachedBuffer, OakDetachedBuffer> next() {
            advance(true);
            return new AbstractMap.SimpleImmutableEntry<>(getKeyDetachedBuffer(ctx), getValueDetachedBuffer(ctx));
        }
    }

    class EntryStreamIterator extends Iter<Map.Entry<OakDetachedBuffer, OakDetachedBuffer>> implements Map.Entry<OakDetachedBuffer, OakDetachedBuffer> {

        private final OakDetachedReadBuffer<KeyBuffer> key = new OakDetachedReadBuffer<>(new KeyBuffer());
        private final OakDetachedReadBuffer<ValueBuffer> value = new OakDetachedReadBuffer<>(new ValueBuffer(valueOperator));

        EntryStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        public Map.Entry<OakDetachedBuffer, OakDetachedBuffer> next() {
            advanceStream(key, value);
            return this;
        }

        @Override
        public OakDetachedBuffer getKey() {
            return key;
        }

        @Override
        public OakDetachedBuffer getValue() {
            return value;
        }

        @Override
        public OakDetachedBuffer setValue(OakDetachedBuffer value) {
            throw new UnsupportedOperationException();
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        EntryTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            assert (transformer != null);
            this.transformer = transformer;
        }

        public T next() {
            advance(true);
            ValueUtils.ValueResult res = valueOperator.lockRead(ctx.value);
            ByteBuffer serializedValue;
            if (res == FALSE) {
                return next();
            } else if (res == RETRY) {
                do {
                    boolean isSuccessful = refreshValuePosition(ctx);
                    if (!isSuccessful) {
                        return next();
                    }
                    res = valueOperator.lockRead(ctx.value);
                } while (res != TRUE);
            }
            serializedValue = valueOperator.getValueByteBufferNoHeaderReadOnly(ctx.value);
            Map.Entry<ByteBuffer, ByteBuffer> entry =
                    new AbstractMap.SimpleEntry<>(ctx.key.getDuplicatedReadByteBuffer(), serializedValue);

            T transformation = transformer.apply(entry);
            valueOperator.unlockRead(ctx.value);
            return transformation;
        }
    }

    // May return deleted keys
    class KeyIterator extends Iter<OakDetachedBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakDetachedBuffer next() {
            advance(false);
            return getKeyDetachedBuffer(ctx);

        }
    }

    public class KeyStreamIterator extends Iter<OakDetachedBuffer> {

        private final OakDetachedReadBuffer<KeyBuffer> key = new OakDetachedReadBuffer<>(new KeyBuffer());

        KeyStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakDetachedBuffer next() {
            advanceStream(key, null);
            return key;
        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        final OakTransformer<T> transformer;

        KeyTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                             OakTransformer<T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            advance(false);
            return transformer.apply(ctx.key.getDuplicatedReadByteBuffer());
        }
    }

    // Factory methods for iterators

    Iterator<OakDetachedBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                         boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<Map.Entry<OakDetachedBuffer, OakDetachedBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive, K hi,
                                                                                        boolean hiInclusive, boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<OakDetachedBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                       boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakDetachedBuffer> valuesStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                     boolean isDescending) {
        return new ValueStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<Map.Entry<OakDetachedBuffer, OakDetachedBuffer>> entriesStreamIterator(K lo, boolean loInclusive, K hi,
                                                                                    boolean hiInclusive, boolean isDescending) {
        return new EntryStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    Iterator<OakDetachedBuffer> keysStreamIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                   boolean isDescending) {
        return new KeyStreamIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> Iterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                            boolean isDescending, OakTransformer<T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                             boolean isDescending,
                                             Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                                          OakTransformer<T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

}
