/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;


import static com.oath.oak.ValueUtils.ValueResult.FALSE;
import static com.oath.oak.ValueUtils.ValueResult.TRUE;

/* EntrySet keeps a set of entries. Entry is reference to key and value, both located off-heap.
** EntrySet provides access, updates and manipulation on each entry, provided its index.
**
** IMPORTANT: Due to limitation in amount of bits we use to represent Block ID the amount of memory
** supported by ALL OAKs in one system is 128GB only!
**
** Entry is a set of 6 (FIELDS) consecutive integers, part of "entries" int array. The definition
** of each integer is explained below. Also bits of each integer are represented in a very special
** way (also explained below). This makes any changes made to this class very delicate. Please
** update with care.
**
** Entries Array:
** --------------------------------------------------------------------------------------
** 0 | headNextIndex keeps entry index of the first ordered entry
** --------------------------------------------------------------------------------------
** 1 | NEXT  - entry index of the entry following the entry with entry index 1  |
** -----------------------------------------------------------------------------|
** 2 | KEY_REFERENCE          | these 2 integers together, represented as long  | entry with
** --|                        | provide KEY_REFERENCE. Pay attention that       | entry index
** 3 |                        | KEY_REFERENCE is different than VALUE_REFERENCE | 1
** -----------------------------------------------------------------------------|
** 4 | VALUE_REFERENCE        | these 2 integers together, represented as long  | entry that
** --|                        | provide VALUE_REFERENCE. Pay attention that     | was allocated
** 5 |                        | VALUE_REFERENCE is different than KEY_REFERENCE | first
 ** ----------------------------------------------------------------------------|
** 6 | VALUE_VERSION - the version of the value required for memory management  |
** --------------------------------------------------------------------------------------
** 7 | NEXT  - entry index of the entry following the entry with entry index 2  |
** -----------------------------------------------------------------------------|
** 8 | KEY_REFERENCE          | these 2 integers together, represented as long  | entry with
** --|                        | provide KEY_REFERENCE. Pay attention that       | entry index
** 9 |                        | KEY_REFERENCE is different than VALUE_REFERENCE | 2
** -----------------------------------------------------------------------------|
** ...
**
**
** Internal class, package visibility */
class EntrySet<K, V> {

    /*-------------- Constants --------------*/

    /***
     * This enum is used to access the different fields in each entry.
     * The value associated with each entry signifies the offset of the field relative to the entry's beginning.
     */
    private enum OFFSET {
        /***
         * NEXT - the next index of this entry (one integer). Must be with offset 0, otherwise, copying an entire
         * entry should be fixed (In function {@code copyPartNoKeys}, search for "LABEL").
         *
         * KEY_REFERENCE - the blockID, length and position of the value pointed from this entry (size of two
         * integers, one long).
         *
         * VALUE_REFERENCE - the blockID, length and position of the value pointed from this entry (size of two
         * integers, one long). Equals to INVALID_REFERENCE if no value is point.
         *
         * VALUE_VERSION - as the name suggests this is the version of the value reference by VALUE_REFERENCE.
         * It initially equals to INVALID_VERSION.
         * If an entry with version v is removed, then this field is CASed to be -v after the value is marked
         * off-heap and the value reference becomes INVALID_VALUE.
         */
        NEXT(0), KEY_REFERENCE(1), VALUE_REFERENCE(3), VALUE_VERSION(5);

        final int value;

        OFFSET(int value) {
            this.value = value;
        }
    }

    public static final int INVALID_VERSION = 0;
    static final int INVALID_ENTRY_INDEX = 0;

    // location of the first (head) node - just a next pointer (always same value 0)
    private final int headNextIndex = 0;

    // index of first item in array, after head (not necessarily first in list!)
    private static final int HEAD_NEXT_INDEX_SIZE = 1;

    private static final int FIELDS = 6;  // # of fields in each item of entries array

    /**
     * Key reference structure:
     *     LSB |     offset      | length | block  | MSB
     *         |     32 bit      | 16 bit | 16 bit |
     * Key limits:
     *   - max buffer size: 4GB
     *   - max allocation length: 64KB
     *   - max number of blocks: 64K
     *   => with the current block size of 256MB, the total memory is up to 128GB
     */
    static final ReferenceCodec KEY = new ReferenceCodec(32, 16, 16);
    public static class KeyBuffer extends OakAttachedReadBuffer {
        public KeyBuffer() {
            super(0);
        }
    }

    /**
     * Value reference structure:
     *     LSB |     offset      |  length   |block| MSB
     *         |     32 bit      |  23 bit   |9 bit|
     * Value limits:
     *   - max buffer size: 4GB
     *   - max allocation length: 8MB
     *   - max number of blocks: 512
     *   => with the current block size of 256MB, the total memory is up to 128GB
     */
    static final ReferenceCodec VALUE = new ReferenceCodec(32, 23, 9);
    public static class ValueBuffer extends OakAttachedReadBuffer {
        protected long reference;

        public ValueBuffer(int headerSize) {
            super(headerSize);
        }

        @Override
        void invalidate() {
            super.invalidate();
            reference = ReferenceCodec.INVALID_REFERENCE;
        }

        void copyFrom(ValueBuffer alloc) {
            if (alloc == this) {
                // No need to do anything if the input is this object
                return;
            }
            super.copyFrom(alloc);
            this.reference = alloc.reference;
        }
    }

    private static final Unsafe unsafe = UnsafeUtils.unsafe;
    private final MemoryManager memoryManager;

    private final int[] entries;    // array is initialized to 0 - this is important!
    private final int entriesCapacity; // number of entries (not ints) to be maximally held

    private final AtomicInteger nextFreeIndex;    // points to next free index of entry array
    // counts number of entries inserted & not deleted, pay attention that not all entries counted
    // in number of entries are finally linked into the linked list of the chunk above
    // and participating in holding the "real" KV-mappings, the "real" are counted in Chunk
    private final AtomicInteger numOfEntries;
    // for writing the keys into the off-heap bytebuffers (Blocks)
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;

    private final ValueUtils valOffHeapOperator; // is used for any value off-heap metadata access

    /*----------------- Constructor -------------------*/
    /**
     * Create a new EntrySet
     * @param memoryManager   for off-heap accesses and updates
     * @param entriesCapacity how many entries should this EntrySet keep at maximum
     * @param keySerializer   used to serialize the key when written to off-heap
     */
    EntrySet(MemoryManager memoryManager, int entriesCapacity, OakSerializer<K> keySerializer,
        OakSerializer<V> valueSerializer, ValueUtils valOffHeapOperator) {
        this.memoryManager   = memoryManager;
        this.entries         = new int[entriesCapacity * FIELDS + HEAD_NEXT_INDEX_SIZE];
        this.nextFreeIndex = new AtomicInteger(HEAD_NEXT_INDEX_SIZE);
        this.numOfEntries = new AtomicInteger(0);
        this.entriesCapacity = entriesCapacity;
        this.keySerializer   = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valOffHeapOperator = valOffHeapOperator;
    }


    /********************************************************************************************/
    /*---------------- Methods for setting and getting specific entry's field ------------------*/
    // we use term intIdx for the integer's index inside the entries array (referred as a set of integers),
    // we use term entryIdx for the index of entry array (referred as a set of entries)

    private static int entryIdx2intIdx(int entryIdx) {
        if (entryIdx == 0) { // assumed it is a head pointer access
            return 0;
        }
        return ((entryIdx-1) * FIELDS) + HEAD_NEXT_INDEX_SIZE;
    }

    /**
     * getEntryArrayFieldInt gets the integer field of entry at specified offset for given
     * start of the entry index in the entry array.
     * The field is read atomically as it is a machine word, however the concurrency of the
     * mostly updated value is not ensured as no memory fence is issued.
     */
    private int getEntryArrayFieldInt(int intFieldIdx, OFFSET offset) {
        return entries[intFieldIdx + offset.value]; // used for NEXT and VALUE_VERSION
    }

    /**
     * getEntryArrayFieldLong atomically reads two integers field of the entries array.
     * Should be used with OFFSET.VALUE_REFERENCE and OFFSET.KEY_REFERENCE
     * The concurrency is ensured due to memory fence as part of "volatile"
     */
    private long getEntryArrayFieldLong(int intStartFieldIdx, OFFSET offset) {
        assert offset == OFFSET.VALUE_REFERENCE || offset == OFFSET.KEY_REFERENCE;
        long arrayOffset =
            Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        return unsafe.getLongVolatile(entries, arrayOffset);
    }

    /**
     * setEntryFieldInt sets the integer field of specified offset to 'value'
     * for given integer index in the entry array
     */
    private void setEntryFieldInt(int intFieldIdx, OFFSET offset, int value) {
        assert intFieldIdx + offset.value >= 0;
        entries[intFieldIdx + offset.value] = value; // used for NEXT and VALUE_VERSION
    }

    /**
     * Sets the two integers field of specified offset to 'value'
     * for given integer index in the entry array
     */
    private void setEntryFieldLongVolatile(int item, OFFSET offset, long value) {
        long arrayOffset = Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        unsafe.putLongVolatile(entries, arrayOffset, value);
    }

    private void setEntryFieldLong(int item, OFFSET offset, long value) {
        long arrayOffset = Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE;
        assert arrayOffset % 8 == 0;
        unsafe.putLong(entries, arrayOffset, value);
    }

    /**
     * casEntriesArrayInt performs CAS of given integer field of the entries ints array,
     * that should be associated with some field of some entry.
     * CAS from 'expectedIntValue' to 'newIntValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayInt(
        int intFieldIdx, OFFSET offset, int expectedIntValue, int newIntValue) {
        return unsafe.compareAndSwapInt(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedIntValue, newIntValue);
    }

    /**
     * casEntriesArrayLong performs CAS of given two integers field of the entries ints array,
     * that should be associated with some 2 consecutive integer fields of some entry.
     * CAS from 'expectedLongValue' to 'newLongValue' for field at specified offset
     * of given int-field in the entries array
     */
    private boolean casEntriesArrayLong(
        int intStartFieldIdx, OFFSET offset, long expectedLongValue, long newLongValue) {
        return unsafe.compareAndSwapLong(entries,
                Unsafe.ARRAY_INT_BASE_OFFSET + (intStartFieldIdx + offset.value) * Unsafe.ARRAY_INT_INDEX_SCALE,
                expectedLongValue, newLongValue);
    }

    /********************************************************************************************/
    /*--------------- Methods for setting and getting specific key and/or value ----------------*/
    // key or value references are the triple <blockID, position, length> encapsulated in long
    // whenever version is needed it is an additional integer

    /**
     * Atomically reads the value version from the entry (given by entry index "ei")
     * synchronisation with reading the value reference is not ensured in this method
     * */
    private int getValueVersion(int ei) {
        return getEntryArrayFieldInt(entryIdx2intIdx(ei), OFFSET.VALUE_VERSION);
    }

    /**
     * Atomically reads the value reference from the entry (given by entry index "ei")
     * synchronisation with reading the value version is not ensured in this method
     * */
    private long getValueReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.VALUE_REFERENCE);
    }

    /**
     * Atomically reads both the value reference and its value (comparing the version).
     * It does that by using the atomic snapshot technique (reading the version, then the reference,
     * and finally the version again, checking that it matches the version read previously).
     * Since a snapshot is used, the LP is when reading the value reference, if the versions match,
     * otherwise, the operation restarts.
     *
     * @param value The value will be returned in this object
     * @param ei    The entry of which the reference and version are read from
     */
    private boolean getValueReferenceAndVersion(ValueBuffer value, int ei) {
        int version;
        long reference;

        do {
            version = getValueVersion(ei);
            // getValueReference() assumes the reference is volatile, so we have a memory fence here
            reference = getValueReference(ei);
        } while (version != getValueVersion(ei));

        boolean isValid = VALUE.decode(value, reference);
        value.reference = reference;
        value.setAllocVersion(version);
        return isValid;
    }

    /**
     * Atomically reads the key reference from the entry (given by entry index "ei")
     */
    private long getKeyReference(int ei) {
        return getEntryArrayFieldLong(entryIdx2intIdx(ei), OFFSET.KEY_REFERENCE);
    }

    /********************************************************************************************/
    /*------------- Methods for managing next entry indexes (package visibility) ---------------*/

    /**
     * getNextEntryIndex returns the next entry index (of the entry given by entry index "ei")
     * The method serves external EntrySet users.
     */
    int getNextEntryIndex(int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            return INVALID_ENTRY_INDEX;
        }
        return getEntryArrayFieldInt(entryIdx2intIdx(ei), OFFSET.NEXT);
    }

    /**
     * getHeadEntryIndex returns the entry index of the entry first in the array,
     * which is written in the first integer of the array
     * The method serves external EntrySet users.
     */
    int getHeadNextIndex() {
        return getEntryArrayFieldInt(headNextIndex, OFFSET.NEXT);
    }

    /**
     * setNextEntryIndex sets the next entry index (of the entry given by entry index "ei")
     * to be the "next". Input parameter "next" must be a valid entry index (not integer index!).
     * The method serves external EntrySet users.
     */
    void setNextEntryIndex(int ei, int next) {
        assert ei <= nextFreeIndex.get() && next <= nextFreeIndex.get();
        setEntryFieldInt(entryIdx2intIdx(ei), OFFSET.NEXT, next);
    }

    /**
     * casNextEntryIndex CAS the next entry index (of the entry given by entry index "ei") to be the
     * "nextNew" only if it was "nextOld". Input parameter "nextNew" must be a valid entry index.
     * The method serves external EntrySet users.
     */
    boolean casNextEntryIndex(int ei, int nextOld, int nextNew) {
        return casEntriesArrayInt(entryIdx2intIdx(ei), OFFSET.NEXT, nextOld, nextNew);
    }

    /********************************************************************************************/
    /*-------------- Methods for managing keys and values of a specific entry' -----------------*/
    // Memory Manager interface is via Slice, although Slices are not key/value containers in the
    // EntrySet. MM allocates/releases Slices

    /**
     * Updates the key portion of the look-up context inside {@code ctx} from an entry given by entry index {@code ei}.
     * This context can be referred to later, and might be followed up by {@code valueLookUp()}.
     */
    boolean keyLookUp(ThreadContext ctx, int ei) {
        ctx.entryIndex = ei;
        return readKey(ctx.key, ei);
    }

    /**
     * Updates the value portion of the look up context inside {@code ctx} that matches the look-up context key.
     * Thus, {@code keyLookUp()} should be called prior to this method with the same {@code ctx} instance.
     */
    void valueLookUp(ThreadContext ctx) {
        boolean isValid = readValue(ctx.value, ctx.entryIndex);

        /*
         The value's allocation version indicate the status of the value referenced by {@code value.reference}.
         If {@code value.reference == INVALID_REFERENCE}, then:
         {@code version <= INVALID_VERSION} if the removal was completed.
         {@code version > INVALID_VERSION} if the removal was not completed.
         otherwise:
         {@code version <= INVALID_VERSION} if the insertion was not completed.
         {@code version > INVALID_VERSION} if the insertion was completed.
         */

        if (!isValid) {
            // There is no value associated with the given key
            // we can be in the middle of insertion or in the middle of removal
            // insert: (1)reference+version=invalid, (2)reference set, (3)version set
            //          middle state valid reference, but invalid version
            // remove: (1)off-heap delete bit, (2)reference invalid, (3)version negative
            //          middle state invalid reference, valid version

            // if version is negative no need to finalize delete
            ctx.valueState = (ctx.value.getAllocVersion() < INVALID_VERSION) ?
                ThreadContext.ValueState.MARKED_DELETED :
                ThreadContext.ValueState.MARKED_DELETED_NOT_FINALIZED;
        } else if (ctx.value.getAllocVersion() <= INVALID_VERSION) {
            /*
             * When value is connected to entry, first the value reference is CASed to the new one and after
             * the value version is set to the new one (written off-heap). Inside entry, when value reference
             * is invalid its version can only be invalid (0) or negative. When value reference is valid and
             * its version is either invalid (0) or negative, the insertion or deletion of the entry wasn't
             * accomplished, and needs to be accomplished.
             */
            ctx.valueState = ThreadContext.ValueState.VALID_INSERT_NOT_FINALIZED;
        } else {
            ValueUtils.ValueResult result = valOffHeapOperator.isValueDeleted(ctx.value);
            if (result == TRUE) {
                // There is a deleted value associated with the given key
                ctx.valueState = ThreadContext.ValueState.MARKED_DELETED_NOT_FINALIZED;
            } else {
                // If result == RETRY, we ignore it, since it will be discovered later down the line as well
                ctx.valueState = ThreadContext.ValueState.VALID;
            }
        }
    }

    /**
     * writeKey writes given key object "key" (to off-heap) as a serialized key, referenced by entry
     * at the entries index "ei"
     **/
    private void writeKey(ThreadContext ctx, K key, int ei) {
        int keySize = keySerializer.calculateSize(key);
        int intIdx = entryIdx2intIdx(ei);
        memoryManager.allocate(ctx.key, keySize, MemoryManager.Allocate.KEY);
        // byteBuffer.slice() is set so it protects us from the overwrites of the serializer
        // TODO: better serializer need to be given OakWBuffer and not ByteBuffer
        keySerializer.serialize(key, ctx.key.getAllocByteBuffer().slice());
        ctx.entryIndex = ei;
        setEntryFieldLong(intIdx, OFFSET.KEY_REFERENCE, KEY.encode(ctx.key));
    }

    /**
     * Reads a key from entry at the given entry index (from off-heap).
     * Returns false if:
     *   (1) there is no such entry or
     *   (2) entry has no key set
     * The thread-local ByteBuffer can be reused by different threads, however as long as
     * a thread is invoked the ByteBuffer is related solely to this thread.
     */
    boolean readKey(KeyBuffer key, int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            key.invalidate();
            return false;
        }

        long reference = getKeyReference(ei);
        boolean isValid = KEY.decode(key, reference);
        if (isValid) {
            memoryManager.readByteBuffer(key);
        }
        return isValid;
    }

    /**
     * Reads a value from entry at the given entry index (from off-heap).
     * Returns false if:
     *   (1) there is no such entry or
     *   (2) entry has no value set
     * The thread-local ByteBuffer can be reused by different threads, however as long as
     * a thread is invoked the ByteBuffer is related solely to this thread.
     */
    boolean readValue(ValueBuffer value, int ei) {
        if (ei == INVALID_ENTRY_INDEX) {
            value.invalidate();
            return false;
        }

        boolean isValid = getValueReferenceAndVersion(value, ei);
        if (isValid) {
            memoryManager.readByteBuffer(value);
        }
        return isValid;
    }

    /**
     * Releases the key of the input context.
     * Currently in use only for unreached keys, waiting for GC to be arranged
     *
     * @param ctx
     **/
    void releaseKey(ThreadContext ctx) {
        memoryManager.release(ctx.key);
    }

    /**
     * Releases the newly allocated value of the input context.
     * Currently the method is used only to release an
     * unreachable value reference, the one that was not yet attached to an entry!
     * The method is part of EntrySet, because it cares also
     * for writing the value before attaching it to an entry (writeValueStart/writeValueCommit)
     **/
    void releaseNewValue(ThreadContext ctx) {
        memoryManager.release(ctx.newValue);
    }

    /**
     * writeValueStart writes value off-heap. Supposed to be for entry index "ei",
     * but this entry metadata is not updated in this method. This is an intermediate step in
     * the process of inserting key-value pair, it will be finished with writeValueCommit.
     * The off-heap header is initialized in this function as well.
     *
     * @param ctx the context that follows the operation since the key being found.
     *               Needed here for the entry index (of the entry to be updated
     *               when insertion of this key-value pair will be committed) and for the old version.
     * @param value the value to write off-heap
     * @param writeForMove
     **/
    void writeValueStart(ThreadContext ctx, V value, boolean writeForMove) {
        // the length of the given value plus its header
        int valueLength = valueSerializer.calculateSize(value) + valOffHeapOperator.getHeaderSize();

        // The allocated slice is actually the thread's ByteBuffer moved to point to the newly
        // allocated slice. Version in time of allocation is set as part of the slice data.
        memoryManager.allocate(ctx.newValue, valueLength, MemoryManager.Allocate.VALUE);
        ctx.newValue.reference = VALUE.encode(ctx.newValue);

        // for value written for the first time:
        // initializing the off-heap header (version and the lock to be free)
        // for value being moved, initialize the lock to be locked
        if (writeForMove) {
            valOffHeapOperator.initLockedHeader(ctx.newValue);
        } else {
            valOffHeapOperator.initHeader(ctx.newValue);

            // Invalidates the "old" value for the future commit
            ctx.value.invalidate();
        }

        // since this is a private environment, we can only use ByteBuffer::slice, instead of ByteBuffer::duplicate
        // and then ByteBuffer::slice
        // To be safe we create a new ByteBuffer object (for the serializer).
        valueSerializer.serialize(value, valOffHeapOperator.getValueByteBufferNoHeaderPrivate(ctx.newValue));
    }

    /**
     * writeValueCommit does the physical CAS of the value reference, which is the Linearization
     * Point of the insertion. It then tries to complete the insertion by CASing the value's version
     * if was not yet assigned (@see #writeValueFinish(LookUp)).
     *
     * @param ctx - holds the entry to which the value reference is linked, the old and new value
     *                references and the old and new value versions.
     * @return {@code true} if the value reference was CASed successfully.
     */
    ValueUtils.ValueResult writeValueCommit(ThreadContext ctx) {
        // If the commit is for a new value, we already invalidated the old value reference and version.
        long oldValueReference = ctx.value.reference;
        long newValueReference = ctx.newValue.reference;
        int oldValueVersion = ctx.value.getAllocVersion();
        int newValueVersion = ctx.newValue.getAllocVersion();
        assert newValueReference != ReferenceCodec.INVALID_REFERENCE;
        int intIdx = entryIdx2intIdx(ctx.entryIndex);
        if (!casEntriesArrayLong(intIdx, OFFSET.VALUE_REFERENCE,
            oldValueReference, newValueReference)) {
            return FALSE;
        }
        casEntriesArrayInt(intIdx, OFFSET.VALUE_VERSION, oldValueVersion, newValueVersion);
        return TRUE;
    }

    /**
     * getNumOfEntries returns the number of entries allocated and not deleted for this EntrySet.
     * Although, in case EntrySet is used as an array, nextFreeIndex is can be used to calculate
     * number of entries, additional variable is used to support OakHash
     */
    int getNumOfEntries() {
        return numOfEntries.get();
    }

    int getLastEntryIndex() {
        return nextFreeIndex.get();
    }

    /**
     * writeValueFinish completes the insertion of a value to Oak. When inserting a value, the value
     * reference is CASed inside the entry and only then the version is CASed. Thus, there can be a
     * time in which the entry's value version is INVALID_VERSION or a negative one. In this method,
     * the version is CASed to complete the insertion.
     *
     * writeValueFinish is used in cases when in an entry the value reference and its off-heap and
     * on-heap versions do not match. In this case it is assumed that we are
     * in the middle of committing a value write and need to write the off-heap value on-heap.
     *
     * <p>
     * The version written to entry is the version written in the off-heap memory. There is no worry
     * of concurrent removals since these removals will have to first call this function as well,
     * and they eventually change the version as well.
     *
     * @param ctx - It holds the entry to CAS, the previously written version of this entry
     *               and the value reference from which the correct version is read.
     *
     * If the value's version is {@code INVALID_VERSION} a CAS will not not be preformed.
     * Otherwise, a positive version is written to the entry.
     * <p>
     * Note 1: the version in the input param {@code ctx} is updated in this method to be the
     * updated one if a valid version was returned.
     *
     * Note 2: updating of the entries MUST be under published operation. The invoker of this method
     * is responsible to call it inside the publish/unpublish scope.
     */
    void writeValueFinish(ThreadContext ctx) {
        final int entryVersion = ctx.value.getAllocVersion();

        // This method should not be called when the value is not written yet, or deleted,
        // or in process of being deleted.
        assert ctx.value.isValid();

        // This method should not be called if the value is already linked
        assert entryVersion <= INVALID_VERSION;

        int offHeapVersion = valOffHeapOperator.getOffHeapVersion(ctx.value);
        casEntriesArrayInt(entryIdx2intIdx(ctx.entryIndex), OFFSET.VALUE_VERSION,
            entryVersion, offHeapVersion);
        ctx.value.setAllocVersion(offHeapVersion);
    }

    /**
     * deleteValueFinish completes the deletion of a value in Oak, by marking the value reference in
     * entry, after the on-heap value was already marked as deleted.
     *
     * As written in {@code writeValueFinish(LookUp)}, when updating an entry, the value reference
     * is CASed first and later the value version, and the same applies when removing a value.
     * However, there is another step before deleting an entry (remove a value), it is marking
     * the value off-heap (the LP).
     *
     * deleteValueFinish is used to first CAS the value reference to {@code INVALID_VALUE_REFERENCE}
     * and then CAS the version to be a negative one. Other threads seeing a value marked as deleted
     * call this function before they proceed (e.g., before performing a successful {@code putIfAbsent}).
     *
     * @param ctx - holds the entry to change, the old value reference to CAS out, and the current value version.
     * @return true if the deletion indeed updated the entry to be deleted as a unique operation
     *
     * Note: updating of the entries MUST be under published operation. The invoker of this method
     * is responsible to call it inside the publish/unpublish scope.
     */
    boolean deleteValueFinish(ThreadContext ctx) {
        final int version = ctx.value.getAllocVersion();
        if (version <= INVALID_VERSION) { // version is marked deleted
            return false;
        }
        int indIdx = entryIdx2intIdx(ctx.entryIndex);
        // Scenario: this value space is allocated once again and assigned into the same entry,
        // while this thread is sleeping. So later a valid value reference is CASed to invalid.
        // In order to not allow this scenario happen we must release the
        // value's off-heap slice to memory manager only after deleteValueFinish is done.
        casEntriesArrayLong(indIdx, OFFSET.VALUE_REFERENCE,
            ctx.value.reference, ReferenceCodec.INVALID_REFERENCE);
        if (casEntriesArrayInt(indIdx, OFFSET.VALUE_VERSION, version, -version)) {
            numOfEntries.getAndDecrement();
            // release the slice
            if (ctx.isValueValid()) {
                memoryManager.release(ctx.value);
            }
            return true;
        }
        return false;
    }

    /**
     * allocateEntry creates/allocates an entry for the key. An entry is always associated with a key,
     * therefore the key is written to off-heap and associated with the entry simultaneously.
     * The value of the new entry is set to NULL: <INVALID_VALUE_REFERENCE, INVALID_VERSION></>
     **/
    void allocateEntry(ThreadContext ctx, K key) {
        ctx.invalidate();

        int ei = nextFreeIndex.getAndIncrement();
        if (ei > entriesCapacity) {
            return;
        }
        numOfEntries.getAndIncrement();
        int intIdx = entryIdx2intIdx(ei);
        // key and value must be set before returned
        // setting the value reference to <INVALID_VALUE_REFERENCE, INVALID_VERSION>
        // TODO: should we do the following setting? Because it is to set zero on zero...
        setEntryFieldLongVolatile(intIdx, OFFSET.VALUE_REFERENCE, ReferenceCodec.INVALID_REFERENCE);
        setEntryFieldInt(intIdx, OFFSET.VALUE_VERSION, INVALID_VERSION);

        writeKey(ctx, key, ei);
    }

    boolean isEntryDeleted(ValueBuffer value, int ei) {
        boolean isValid = readValue(value, ei);
        if(!isValid) {
            return true;
        }
        return valOffHeapOperator.isValueDeleted(value) != FALSE;
    }

    /*
     * isValueRefValid is used only to check whether the value reference, which is part of the
     * entry on entry index "ei" is valid. No version check and no off-heap value deletion mark check.
     * Negative version is not checked, because negative version assignment will follow the
     * invalid reference assignment.ass
     * Pay attention that value may be deleted (value reference marked invalid) asynchronously
     * by other thread just after this check. For the thread safety use a copy of value reference (next method.)
     * */
    boolean isValueRefValid(int ei) {
        return ReferenceCodec.isValidReference(getValueReference(ei));
    }

    /******************************************************************/
    /*
     * All the functionality that links entries into a linked list or updates the linked list
     * is provided by the user of the entry set. EntrySet provides the possibility to update the
     * next entry index via set or CAS, but does it only as a result of the user request.
     * */


    /**
     * copyEntry copies one entry from source EntrySet (at source entry index "srcEntryIdx") to this EntrySet.
     * The destination entry index is chosen according to this nextFreeIndex which is increased with
     * each copy. Deleted entry (marked on-heap or off-heap) is not copied (disregarded).
     *
     * The next pointers of the entries are requested to be set by the user if needed.
     *
     * @return false when this EntrySet is full
     *
     * NOT THREAD SAFE
     * */
    boolean copyEntry(ValueBuffer value, EntrySet<K,V> srcEntrySet, int srcEntryIdx) {
        if (srcEntryIdx == headNextIndex) {
            return false;
        }

        // don't increase the nextFreeIndex yet, as the source entry might not be copies
        int destEntryIndex = nextFreeIndex.get();

        if (destEntryIndex > entriesCapacity) {return false;}

        if (srcEntrySet.isEntryDeleted(value, srcEntryIdx)) {return true;}

        // ARRAY COPY: using next as the base of the entry
        // copy both the key and the value references the value's version => 5 integers via array copy
        // the first field in an entry is next, and it is not copied since it should be assigned elsewhere
        // therefore, to copy the rest of the entry we use the offset of next (which we assume is 0) and
        // add 1 to start the copying from the subsequent field of the entry.
        System.arraycopy(srcEntrySet.entries,  // source entries array
            entryIdx2intIdx(srcEntryIdx) + OFFSET.NEXT.value + 1,
            entries,                        // this entries array
            entryIdx2intIdx(destEntryIndex) + OFFSET.NEXT.value + 1, (FIELDS - 1));

        // now it is the time to increase nextFreeIndex
        nextFreeIndex.getAndIncrement();
        numOfEntries.getAndIncrement();
        return true;
    }


}
