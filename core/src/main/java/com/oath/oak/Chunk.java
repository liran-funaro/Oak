/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.EmptyStackException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

import static com.oath.oak.ValueUtils.ValueResult.*;

class Chunk<K, V> {
    static final int NONE_NEXT = 0;    // an entry with NONE_NEXT as its next pointer, points to a null entry

    /*-------------- Constants --------------*/

    enum State {
        INFANT,
        NORMAL,
        FROZEN,
        RELEASED
    }

    // used for checking if rebalance is needed
    private static final double REBALANCE_PROB_PERC = 30;
    private static final double SORTED_REBALANCE_RATIO = 2;
    private static final double MAX_ENTRIES_FACTOR = 2;
    private static final double MAX_IDLE_ENTRIES_FACTOR = 5;
    private static final int INVALID_ANCHOR_INDEX = -1;

    // defaults
    public static final int MAX_ITEMS_DEFAULT = 4096;

    private static final Unsafe unsafe = UnsafeUtils.unsafe;
    ByteBuffer minKey;       // minimal key that can be put in this chunk
    AtomicMarkableReference<Chunk<K, V>> next;
    OakComparator<K> comparator;

    // in split/compact process, represents parent of split (can be null!)
    private AtomicReference<Chunk<K, V>> creator;
    // chunk can be in the following states: normal, frozen or infant(has a creator)
    private final AtomicReference<State> state;
    private AtomicReference<Rebalancer<K, V>> rebalancer;
    private final EntrySet<K,V> entrySet;

    private AtomicInteger pendingOps;

    private final Statistics statistics;
    // # of sorted items at entry-array's beginning (resulting from split)
    private AtomicInteger sortedCount;
    private final int maxItems;
    AtomicInteger externalSize; // for updating oak's size (reference to one global per Oak size)

    /*-------------- Constructors --------------*/

    /**
     * Create a new chunk
     *
     * @param minKey  minimal key to be placed in chunk
     * @param creator the chunk that is responsible for this chunk creation
     */
    Chunk(ByteBuffer minKey, Chunk<K, V> creator, OakComparator<K> comparator, MemoryManager memoryManager,
          int maxItems, AtomicInteger externalSize, OakSerializer<K> keySerializer, OakSerializer<V> valueSerializer,
          ValueUtils valueOperator) {

        this.maxItems = maxItems;
        this.entrySet =
            new EntrySet<K,V>(memoryManager, maxItems, keySerializer, valueSerializer,
                valueOperator);
        // if not zero, sorted count keeps the entry index of the last
        // subsequent and ordered entry in the entries array
        this.sortedCount = new AtomicInteger(0);
        this.minKey = minKey;
        this.creator = new AtomicReference<>(creator);
        if (creator == null) {
            this.state = new AtomicReference<>(State.NORMAL);
        } else {
            this.state = new AtomicReference<>(State.INFANT);
        }
        this.next = new AtomicMarkableReference<>(null, false);
        this.pendingOps = new AtomicInteger();
        this.rebalancer = new AtomicReference<>(null); // to be updated on rebalance
        this.statistics = new Statistics();
        this.comparator = comparator;
        this.externalSize = externalSize;
    }

    /*-------------- Methods --------------*/

    void release() {
        // try to change the state
        state.compareAndSet(State.FROZEN, State.RELEASED);
    }

    boolean readMinKey(EntrySet.KeyBuffer key) {
        return entrySet.readKey(key, entrySet.getHeadNextIndex());
    }

    boolean readMaxKey(EntrySet.KeyBuffer key) {
        int maxEntry = getLastItemEntryIndex();
        return entrySet.readKey(key, maxEntry);
    }

    /**
     * look up key
     *
     * @param ctx a context object to update with the results
     * @param key the key to look up
     *
     * The ctx will describe the condition of the value associated with {@code key}.
     * If {@code (ctx.isKeyValid() && ctx.isValueValid()) == True}, the key and value was found.
     * If {@code ctx.isKeyValid() == False}, there is no entry with that key in this chunk.
     * Otherwise, if {@code lookup.isValueValid() == False}, it means that there is an entry with that key (can be
     * reused in case
     * of {@code put}, but there is no value attached to this key (it can happen if this entry is in the midst of
     * being inserted, or some thread removed this key and no rebalanace occurred leaving the entry in the chunk).
     * It means that the value is marked off-heap as deleted, but the connection between the entry and the value was not unlinked yet.
     * {@code ctx.valueState} will indicate the reason.
     */
    void lookUp(ThreadContext ctx, K key) {
        // binary search sorted part of key array to quickly find node to start search at
        // it finds previous-to-key
        int curr = binaryFind(ctx.key, key);
        curr = (curr == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(curr);

        // iterate until end of list (or key is found)
        while (curr != NONE_NEXT) {
            // compare current item's key to searched key
            boolean isValid = entrySet.keyLookup(ctx, curr);
            assert isValid;
            int cmp = comparator.compareKeyAndSerializedKey(key, ctx.key.getAllocByteBuffer());
            // if item's key is larger - we've exceeded our key
            // it's not in chunk - no need to search further
            if (cmp < 0) {
                // Reset lookup key state to INVALID
                ctx.invalidate();
                return;
            }
            // if keys are equal - we've found the item
            else if (cmp == 0) {
                // Update the lookup value state
                entrySet.valueLookUp(ctx);
                return;
            }
            // otherwise- proceed to next item
            curr = entrySet.getNextEntryIndex(curr);
        }

        // Reset lookup key state to INVALID
        ctx.invalidate();
    }

    /**
     * This function completes the insertion (or deletion) of a value to Entry. When inserting a
     * value, the value reference is CASed inside the entry first and only afterwards the version is
     * CASed. Thus, there can be a time in which the value reference is valid but the version is
     * INVALID_VERSION or a negative one. In this function, the version is CASed to complete the
     * insertion.
     * <p>
     * The version written to entry is the version written in the off-heap memory. There is no worry
     * of concurrent removals since these removals will have to first call this function as well,
     * and they eventually change the version as well.
     *
     * @param ctx - It holds the entry to CAS, the value version written in this entry and the
     *               value reference from which the correct version can be read.
     * @return a version is returned.
     * If it is {@code INVALID_VERSION} it means that a CAS was not preformed. Otherwise, a positive version is
     * returned, and it the version written to the entry (maybe by some other thread).
     * <p>
     * Note that the version in the input param {@code ctx} is updated to be the right one if a valid version was
     * returned.
     */
    int completeLinking(ThreadContext ctx) {
        if (!ctx.isDeleteValueFinishNeeded() && ctx.isValueLinkFinished()) {
            // the version written in lookup is a good one!
            return ctx.value.getAllocVersion();
        }
        if (!publish()) {
            return Slice.INVALID_VERSION;
        }
        try {
            entrySet.writeValueFinish(ctx);
        } finally {
            unpublish();
        }

        return ctx.value.getAllocVersion();
    }

    /**
     * As written in {@code writeValueFinish(LookUp)}, when changing an entry, the value reference is CASed first and
     * later the value version, and the same applies when removing a value. However, there is another step before
     * changing an entry to remove a value and it is marking the value off-heap (the LP). This function is used to
     * first CAS the value reference to {@code INVALID_VALUE_REFERENCE} and then CAS the version to be a negative one.
     * Other threads seeing a marked value call this function before they proceed (e.g., before performing a
     * successful {@code putIfAbsent}).
     *
     * @param ctx - holds the entry to change, the old value reference to CAS out, and the current value version.
     * @return {@code true} if a rebalance is needed
     */
    boolean finalizeDeletion(ThreadContext ctx) {

        if (!ctx.isDeleteValueFinishNeeded()) {
            return false;
        }
        if (!publish()) {
            return true;
        }
        try {
            if (!entrySet.deleteValueFinish(ctx)) {
                return false;
            }
            externalSize.decrementAndGet();
            statistics.decrementAddedCount();
            return false;
        } finally {
            unpublish();
        }
    }

    // Release context's key, currently not in use, waiting for GC to be arranged
    void releaseKey(ThreadContext ctx) {
        entrySet.releaseKey(ctx);
    }

    // Release context's newly allocated value
    void releaseNewValue(ThreadContext ctx) {
        entrySet.releaseNewValue(ctx);
    }

    // Check if value reference is valid. Doesn't check further than that
    // (meaning whether the underlined off-heap is marked deleted or version is negative)
    boolean isValueRefValid(int ei) {
      return entrySet.isValueRefValid(ei);
    }

    boolean keyLookUp(ThreadContext ctx, int ei) {
        return entrySet.keyLookup(ctx, ei);
    }

    void valueLookUp(ThreadContext ctx) {
        entrySet.valueLookUp(ctx);
    }

    boolean readKeyFromEntryIndex(EntrySet.KeyBuffer key, int ei) {
        return entrySet.readKey(key, ei);
    }

    boolean readValueFromEntryIndex(EntrySet.ValueBuffer value, int ei) {
        return entrySet.readValue(value, ei);
    }

    /**
     * binary search for largest-entry smaller than 'key' in sorted part of key array.
     *
     * @return the index of the entry from which to start a linear search -
     * if key is found, its previous entry is returned!
     * In cases when search from the head is needed, meaning:
     * (1) the given key is less or equal than the smallest key in the chunk OR
     * (2) entries are unsorted so there is a need to start from the beginning of the linked list
     * NONE_NEXT is going to be returned
     */
    private int binaryFind(EntrySet.KeyBuffer keyBuff, K key) {
        int sortedCount = this.sortedCount.get();
        // if there are no sorted keys, return the head entry for a regular linear search
        if (sortedCount == 0) {
            return NONE_NEXT;
        }

        int headIdx = entrySet.getHeadNextIndex();
        entrySet.readKey(keyBuff, headIdx);
        // if the first item is already larger than key,
        // return the head entry for a regular linear search
        if (comparator.compareKeyAndSerializedKey(key, keyBuff.getAllocByteBuffer()) <= 0) {
            return NONE_NEXT;
        }

        // optimization: compare with last key to avoid binary search (here sortedCount is not zero)
        entrySet.readKey(keyBuff, sortedCount);
        if (comparator.compareKeyAndSerializedKey(key, keyBuff.getAllocByteBuffer()) > 0) {
            return sortedCount;
        }

        int start = 0;
        int end = sortedCount;
        while (end - start > 1) {
            int curr = start + (end - start) / 2;
            entrySet.readKey(keyBuff, curr);
            if (comparator.compareKeyAndSerializedKey(key, keyBuff.getAllocByteBuffer()) <= 0) {
                end = curr;
            } else {
                start = curr;
            }
        }

        return start;
    }

    /**
     * publish operation into thread array
     * if CAS didn't succeed then this means that a rebalancer got here first and entry is frozen
     *
     * @return result of CAS
     **/
    boolean publish() {
        pendingOps.incrementAndGet();
        State currentState = state.get();
        if (currentState == State.FROZEN || currentState == State.RELEASED) {
            pendingOps.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * unpublish operation from thread array
     * if CAS didn't succeed then this means that a rebalancer did this already
     **/
    void unpublish() {
        pendingOps.decrementAndGet();
    }

    void allocateEntryAndKey(ThreadContext ctx, K key) {
        entrySet.allocateEntry(ctx, key);
    }

    int linkEntry(ThreadContext ctx, K key) {
        int prev, curr, cmp;
        int anchor = INVALID_ANCHOR_INDEX;
        final int ei = ctx.entryIndex;
        final EntrySet.KeyBuffer keyBuff = ctx.tempKey;
        while (true) {
            // start iterating from quickly-found node (by binary search) in sorted part of order-array
            if (anchor == INVALID_ANCHOR_INDEX) {
                anchor = binaryFind(keyBuff, key);
            }
            if (anchor == NONE_NEXT) {
                prev = NONE_NEXT;
                curr = entrySet.getHeadNextIndex();
            } else {
                prev = anchor;
                curr = entrySet.getNextEntryIndex(anchor);    // index of next item in list
            }

            //TODO: use ctx and location window inside ctx (when key wasn't found),
            //TODO: so there us no need to iterate again in linkEntry
            // iterate items until key's position is found
            while (true) {
                // if no item, done searching - add to end of list
                if (curr == NONE_NEXT) {
                    break;
                }
                // compare current item's key to ours
                entrySet.readKey(keyBuff, curr);
                cmp = comparator.compareKeyAndSerializedKey(key, keyBuff.getAllocByteBuffer());

                // if current item's key is larger, done searching - add between prev and curr
                if (cmp < 0) {
                    break;
                }

                // if same key, someone else managed to add the key to the linked list
                if (cmp == 0) {
                    return curr;
                }

                prev = curr;
                curr = entrySet.getNextEntryIndex(prev);    // index of next item in list
            }

            // link to list between curr and previous, first change this entry's next to point to curr
            // no need for CAS since put is not even published yet
            entrySet.setNextEntryIndex(ei, curr);
            if (entrySet.casNextEntryIndex(prev, curr, ei)) {
                // Here is the single place where we do enter a new entry to the chunk, meaning
                // there is none else who can simultaneously insert the same key
                // (we were the first to insert this key).
                // If the new entry's index is exactly after the sorted count and
                // the entry's key is greater or equal then to the previous (sorted count)
                // index key. Then increase the sorted count.
                int sortedCount = this.sortedCount.get();
                if (sortedCount > 0) {
                    if (ei == (sortedCount + 1)) { // first entry has entry index 1, not 0
                        // the new entry's index is exactly after the sorted count
                        entrySet.readKey(keyBuff, sortedCount);
                        if (comparator.compareKeyAndSerializedKey(
                                key, keyBuff.getAllocByteBuffer()) >= 0) {
                            // compare with sorted count key, if inserting the "if-statement",
                            // the sorted count key is less or equal to the key just inserted
                            this.sortedCount.compareAndSet(sortedCount, (sortedCount + 1));
                        }
                    }
                }
                return ei;
            }
            // CAS didn't succeed, try again
        }
    }

    /**
     * write value off-heap, promoted to EntrySet.
     *
     * @param ctx to be used later in the writeValueCommit
     * @param value the value to write off-heap
     * @param writeForMove
     **/
    void writeValue(ThreadContext ctx, V value, boolean writeForMove) {
        entrySet.writeValueStart(ctx, value, writeForMove);
    }


    int getMaxItems() {
        return maxItems;
    }

    /**
     * This function does the physical CAS of the value reference, which is the LP of the insertion. It then tries to
     * complete the insertion (@see #writeValueFinish(LookUp)).
     * This is also the only place in which the size of Oak is updated.
     *
     * @param ctx - holds the entry to which the value reference is linked, the old and new value references and
     *               the old and new value versions.
     * @return {@code true} if the value reference was CASed successfully.
     */
    ValueUtils.ValueResult linkValue(ThreadContext ctx) {
        if (entrySet.writeValueCommit(ctx) == FALSE) {
            return FALSE;
        }

        // If the old value is invalid, the link is for a new value (not a move)
        if (!ctx.isValueValid()) {
            statistics.incrementAddedCount();
            externalSize.incrementAndGet();
        }
        return TRUE;
    }

    /**
     * Engage the chunk to a rebalancer r.
     *
     * @param r -- a rebalancer to engage with
     */
    void engage(Rebalancer<K, V> r) {
        rebalancer.compareAndSet(null, r);
    }

    /**
     * Checks whether the chunk is engaged with a given rebalancer.
     *
     * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
     * @return true if the chunk is engaged with r, false otherwise
     */
    boolean isEngaged(Rebalancer<K, V> r) {
        return rebalancer.get() == r;
    }

    /**
     * Fetch a rebalancer engaged with the chunk.
     *
     * @return rebalancer object or null if not engaged.
     */
    Rebalancer<K, V> getRebalancer() {
        return rebalancer.get();
    }

    Chunk<K, V> creator() {
        return creator.get();
    }

    State state() {
        return state.get();
    }

    private void setState(State state) {
        this.state.set(state);
    }

    void normalize() {
        state.compareAndSet(State.INFANT, State.NORMAL);
        creator.set(null);
        // using fence so other puts can continue working immediately on this chunk
        Chunk.unsafe.storeFence();
    }

    final int getFirstItemEntryIndex() {
        return entrySet.getHeadNextIndex();
    }

    private int getLastItemEntryIndex() {
        // find the last sorted entry
        int sortedCount = this.sortedCount.get();
        int entryIndex = sortedCount == 0 ? entrySet.getHeadNextIndex() : sortedCount;
        int nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        while (nextEntryIndex != NONE_NEXT) {
            entryIndex = nextEntryIndex;
            nextEntryIndex = entrySet.getNextEntryIndex(entryIndex);
        }
        return entryIndex;
    }

    /**
     * freezes chunk so no more changes can be done to it (marks pending items as frozen)
     */
    void freeze() {
        setState(State.FROZEN); // prevent new puts to this chunk
        while (pendingOps.get() != 0) ;
    }

    /***
     * Copies entries from srcChunk (starting srcEntryIdx) to this chunk,
     * performing entries sorting on the fly (delete entries that are removed as well).
     * @param value -- a value buffer to be used by copyEntry()
     * @param srcChunk -- chunk to copy from
     * @param srcEntryIdx -- start position for copying
     * @param maxCapacity -- max number of entries "this" chunk can contain after copy
     * @return entry index of next to the last copied entry (in the srcChunk),
     *              NONE_NEXT if all items were copied
     */
    final int copyPartNoKeys(EntrySet.ValueBuffer value, Chunk<K, V> srcChunk, int srcEntryIdx, int maxCapacity) {

        if (srcEntryIdx == NONE_NEXT) {
            return NONE_NEXT;
        }

        // use local variables and just set the atomic variables once at the end
        int numOfEntries = entrySet.getNumOfEntries();
        // next *free* index of this entries array
        int sortedThisEntryIndex = numOfEntries+1;

        // check that we are not beyond allowed number of entries to copy from source chunk
        if (numOfEntries >= maxCapacity) {
            return srcEntryIdx;
        }
        // assuming that all chunks are bounded with same number of entries to hold
        assert srcEntryIdx <= maxItems;

        // set the next entry index (previous entry or head) from where we start to copy
        // if sortedThisEntryIndex is one (first entry to be occupied on this chunk)
        // we are exactly going to update the head (ei=0)
        entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, sortedThisEntryIndex);

        // Here was the code that was trying to read entries from srcEntryIdx on the source chunk
        // to see how much of them are subject for a copy, ordered and not deleted,
        // so theoretically they can be copied with copy array. The code is removed, because anyway
        // the long copy array doesn't happen since "next" needs to be updated separately.

        // copy entry by entry traversing the source linked list
        while(entrySet.copyEntry(value, srcChunk.entrySet, srcEntryIdx)) {
            // the source entry was either copied or disregarded as deleted
            // anyway move to next source entry (according to the linked list)
            srcEntryIdx = srcChunk.entrySet.getNextEntryIndex(srcEntryIdx);

            // if entry was ignored as deleted (no change in this EntrySet num of entries), continue
            if (numOfEntries == entrySet.getNumOfEntries()) {
                continue;
            }

            // we indeed copied the entry, update the number of entries and the next pointer
            numOfEntries++;
            sortedThisEntryIndex++;
            entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, sortedThisEntryIndex);

            // check that we are not beyond allowed number of entries to copy from source chunk
            if (numOfEntries >= maxCapacity) {
                break;
            }

            // is there something to copy on the source side?
            if (srcEntryIdx == NONE_NEXT) {
                break;
            }
        }
        // we have stopped the copy because (1) this entry set is full, OR (2) ended source entries,
        // OR (3) we copied allowed number of entries

        // the last next pointer was set to what is there in the source to copy, reset it to null
        entrySet.setNextEntryIndex(sortedThisEntryIndex - 1, NONE_NEXT);
        // sorted count keeps the number of sorted entries
        sortedCount.set(numOfEntries);
        statistics.updateInitialSortedCount(sortedCount.get());
        return srcEntryIdx; // if NONE_NEXT then we finished copying old chunk, else we reached max in new chunk
    }

    /**
     * marks this chunk's next pointer so this chunk is marked as deleted
     *
     * @return the next chunk pointed to once marked (will not change)
     */
    Chunk<K, V> markAndGetNext() {
        // new chunks are ready, we mark frozen chunk's next pointer so it won't change
        // since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
        while (true) {
            // if chunk is marked - that is ok and its next pointer will not be changed anymore
            // return whatever chunk is set as next
            if (next.isMarked()) {
                return next.getReference();
            }
            // otherwise try to mark it
            else {
                // read chunk's current next
                Chunk<K, V> savedNext = next.getReference();

                // try to mark next while keeping the same next chunk - using CAS
                // if we succeeded then the next pointer we remembered is set and will not change - return it
                if (next.compareAndSet(savedNext, savedNext, false, true)) {
                    return savedNext;
                }
            }
        }
    }


    boolean shouldRebalance() {
        // perform actual check only in pre defined percentage of puts
        if (ThreadLocalRandom.current().nextInt(100) > REBALANCE_PROB_PERC) {
            return false;
        }

        // if another thread already runs rebalance -- skip it
        if (!isEngaged(null)) {
            return false;
        }
        int numOfEntries = entrySet.getNumOfEntries();
        int numOfItems = statistics.getCompactedCount();
        int sortedCount = this.sortedCount.get();
        // Reasons for executing a rebalance:
        // 1. There are no sorted keys and the total number of entries is above a certain threshold.
        // 2. There are sorted keys, but the total number of unsorted keys is too big.
        // 3. Out of the occupied entries, there are not enough actual items.
        return (sortedCount == 0 && numOfEntries * MAX_ENTRIES_FACTOR > maxItems) ||
                (sortedCount > 0 && (sortedCount * SORTED_REBALANCE_RATIO) < numOfEntries) ||
                (numOfEntries * MAX_IDLE_ENTRIES_FACTOR > maxItems && numOfItems * MAX_IDLE_ENTRIES_FACTOR < numOfEntries);
    }

    /*-------------- Iterators --------------*/

    AscendingIter ascendingIter() {
        return new AscendingIter();
    }

    AscendingIter ascendingIter(ThreadContext ctx, K from, boolean inclusive) {
        return new AscendingIter(ctx, from, inclusive);
    }

    DescendingIter descendingIter(ThreadContext ctx) {
        return new DescendingIter(ctx);
    }

    DescendingIter descendingIter(ThreadContext ctx, K from, boolean inclusive) {
        return new DescendingIter(ctx, from, inclusive);
    }

    private int advanceNextIndex(int next) {
        while (next != NONE_NEXT && !entrySet.isValueRefValid(next)) {
            next = entrySet.getNextEntryIndex(next);
        }
        return next;
    }

    interface ChunkIter {
        boolean hasNext();
        int next(ThreadContext ctx);
    }

    class AscendingIter implements ChunkIter {

        private int next;

        AscendingIter() {
            next = entrySet.getHeadNextIndex();
            next = advanceNextIndex(next);
        }

        AscendingIter(ThreadContext ctx, K from, boolean inclusive) {
            EntrySet.KeyBuffer keyBuff = ctx.tempKey;
            next = binaryFind(keyBuff, from);
            next = (next == NONE_NEXT) ? entrySet.getHeadNextIndex() : entrySet.getNextEntryIndex(next);
            int compare = -1;
            if (next != NONE_NEXT) {
                entrySet.readKey(keyBuff, next);
                compare = comparator.compareKeyAndSerializedKey(from, keyBuff.getAllocByteBuffer());
            }
            while (next != NONE_NEXT &&
                (compare > 0 || (compare >= 0 && !inclusive) || !entrySet.isValueRefValid(next))) {
                next = entrySet.getNextEntryIndex(next);
                if (next != NONE_NEXT) {
                    entrySet.readKey(keyBuff, next);
                    compare = comparator.compareKeyAndSerializedKey(from, keyBuff.getAllocByteBuffer());
                }
            }
        }

        private void advance() {
            next = entrySet.getNextEntryIndex(next);
            next = advanceNextIndex(next);
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance();
            return toReturn;
        }
    }

    class DescendingIter implements ChunkIter {

        private int next;
        private int anchor;
        private int prevAnchor;
        private final IntStack stack;
        private final K from;
        private boolean inclusive;

        static final int SKIP_ENTRIES_FOR_BIGGER_STACK = 1; // 1 is the lowest possible value

        DescendingIter(ThreadContext ctx) {
            EntrySet.KeyBuffer keyBuff = ctx.tempKey;

            from = null;
            stack = new IntStack(entrySet.getLastEntryIndex());
            int sortedCnt = sortedCount.get();
            anchor = // this is the last sorted entry
                (sortedCnt == 0 ? entrySet.getHeadNextIndex() : sortedCnt);
            stack.push(anchor);
            initNext(keyBuff);
        }

        DescendingIter(ThreadContext ctx, K from, boolean inclusive) {
            EntrySet.KeyBuffer keyBuff = ctx.tempKey;

            this.from = from;
            this.inclusive = inclusive;
            stack = new IntStack(entrySet.getLastEntryIndex());
            anchor = binaryFind(keyBuff, from);
            // translate to be valid index, if anchor is head we know to stop the iteration
            anchor = (anchor == NONE_NEXT) ? entrySet.getHeadNextIndex() : anchor;
            stack.push(anchor);
            initNext(keyBuff);
        }

        private void initNext(EntrySet.KeyBuffer keyBuff) {
            traverseLinkedList(keyBuff, true);
            advance(keyBuff);
        }

        /**
         * use stack to find a valid next, removed items can't be next
         */
        private void findNewNextInStack() {
            if (stack.empty()) {
                next = NONE_NEXT;
                return;
            }
            next = stack.pop();
            while (next != NONE_NEXT && !entrySet.isValueRefValid(next)) {
                if (!stack.empty()) {
                    next = stack.pop();
                } else {
                    next = NONE_NEXT;
                    return;
                }
            }
        }

        private void pushToStack(boolean compareWithPrevAnchor) {
            while (next != NONE_NEXT) {
                if (!compareWithPrevAnchor) {
                    stack.push(next);
                    next = entrySet.getNextEntryIndex(next);
                } else {
                    if (next != prevAnchor) {
                        stack.push(next);
                        next = entrySet.getNextEntryIndex(next);
                    } else {
                        break;
                    }
                }
            }
        }

        /**
         * fill the stack
         * @param firstTimeInvocation
         */
        private void traverseLinkedList(EntrySet.KeyBuffer keyBuff, boolean firstTimeInvocation) {
            assert stack.size() == 1;   // ancor is in the stack
            if (prevAnchor == entrySet.getNextEntryIndex(anchor)) {
                next = NONE_NEXT;   // there is no next;
                return;
            }
            next = entrySet.getNextEntryIndex(anchor);
            if (from == null) {
                // if this is not the first invocation, stop when reaching previous anchor
                pushToStack(!firstTimeInvocation);
            } else {
                if (firstTimeInvocation) {
                    int threshold = inclusive ? 0 : 1;
                    // readyKey will return true only if next != NONE_NEXT
                    while (entrySet.readKey(keyBuff, next)) {
                        // This is equivalent to continue while:
                        //         when inclusive: CMP >= 0
                        //     when non-inclusive: CMP > 0
                        if (comparator.compareKeyAndSerializedKey(from, keyBuff.getAllocByteBuffer()) < threshold) {
                            break;
                        }
                        stack.push(next);
                        next = entrySet.getNextEntryIndex(next);
                    }
                } else {
                    // stop when reaching previous anchor
                    pushToStack(true);
                }
            }
        }

        /**
         * find new valid anchor
         */
        private void findNewAnchor() {
            assert stack.empty();
            prevAnchor = anchor;
            if (anchor == entrySet.getHeadNextIndex()) {
                next = NONE_NEXT; // there is no more in this chunk
                return;
            } else if (anchor == 1) { // cannot get below the first index
                anchor = entrySet.getHeadNextIndex();
            } else {
                if ((anchor - SKIP_ENTRIES_FOR_BIGGER_STACK) > 1) {
                    // try to skip more then one backward step at a time
                    // if it shows better performance
                    anchor -= SKIP_ENTRIES_FOR_BIGGER_STACK;
                } else {
                    anchor -= 1;
                }
            }
            stack.push(anchor);
        }

        private void advance(EntrySet.KeyBuffer keyBuff) {
            while (true) {
                findNewNextInStack();
                if (next != NONE_NEXT) {
                    return;
                }
                // there is no next in stack
                if (anchor == entrySet.getHeadNextIndex()) {
                    // there is no next at all
                    return;
                }
                findNewAnchor();
                traverseLinkedList(keyBuff, false);
            }
        }

        @Override
        public boolean hasNext() {
            return next != NONE_NEXT;
        }

        @Override
        public int next(ThreadContext ctx) {
            int toReturn = next;
            advance(ctx.tempKey);
            return toReturn;
        }

    }

    /**
     * just a simple stack of int, implemented with int array
     */

    static class IntStack {

        private final int[] stack;
        private int top;

        IntStack(int size) {
            stack = new int[size];
            top = 0;
        }

        void push(int i) {
            stack[top] = i;
            top++;
        }

        int pop() {
            if (empty()) {
                throw new EmptyStackException();
            }
            top--;
            return stack[top];
        }

        boolean empty() {
            return top == 0;
        }

        int size() {
            return top;
        }

    }

    /*-------------- Statistics --------------*/

    /**
     * This class contains information about chunk utilization.
     */
    static class Statistics {
        private final AtomicInteger addedCount = new AtomicInteger(0);
        private int initialSortedCount = 0;

        /**
         * Initial sorted count here is immutable after chunk re-balance
         */
        void updateInitialSortedCount(int sortedCount) {
            this.initialSortedCount = sortedCount;
        }

        /**
         * @return number of items chunk will contain after compaction.
         */
        int getCompactedCount() {
            return initialSortedCount + getAddedCount();
        }

        /**
         * Incremented when put a key that was removed before
         */
        void incrementAddedCount() {
            addedCount.incrementAndGet();
        }

        /**
         * Decrement when remove a key that was put before
         */
        void decrementAddedCount() {
            addedCount.decrementAndGet();
        }

        int getAddedCount() {
            return addedCount.get();
        }

    }

    /**
     * @return statistics object containing approximate utilization information.
     */
    Statistics getStatistics() {
        return statistics;
    }

}
