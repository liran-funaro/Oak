package com.oath.oak;

/**
 * LookUp is a class that encapsulates the lookup information, from when entry (key) was
 * (or wasn't) found and until the operation was completed.
 */
class ThreadContext {

    enum ValueState {
        /*
         * The state of the value is yet to be checked.
         */
        UNKNOWN,

        /*
         * There is an entry with the given key and its value is deleted (or at least in the process of
         * being deleted, marked just off-heap).
         */
        MARKED_DELETED,

        /*
         * When entry is makred deleted, but not yet suitable to be reused.
         * Deletion consists of 3 steps: (1) mark off-heap deleted (LP),
         * (2) CAS value reference to invalid, (3) CAS value version to negative.
         * If not all three steps are done entry can not be reused for new insertion.
         */
        MARKED_DELETED_NOT_FINALIZED,

        /*
         * There is any entry with the given key and its value is valid.
         * valueSlice is pointing to the location that is referenced by valueReference.
         */
        VALID,

        /*
         * When value is connected to entry, first the value reference is CASed to the new one and after
         * the value version is set to the new one (written off-heap). Inside entry, when value reference
         * is invalid its version can only be invalid (0) or negative. When value reference is valid and
         * its version is either invalid (0) or negative, the insertion or deletion of the entry wasn't
         * accomplished, and needs to be accomplished.
         */
        VALID_INSERT_NOT_FINALIZED,
    }

    /* The index of the thread that this context belongs to */
    final int threadIndex;

    /*-----------------------------------------------------------
     * Entry Lookup Context
     *-----------------------------------------------------------/

    /* The index of the key's entry in EntrySet */
    int entryIndex;

    /* key is used for easier access to the off-heap memory */
    final EntrySet.KeyBuffer key;

    /* The state of the value lookup */
    ValueState valueState;

    /* value is used for easier access to the off-heap memory */
    final EntrySet.ValueBuffer value;

    /*-----------------------------------------------------------
     * Value Insertion Context
     *-----------------------------------------------------------*/

    /**
     * This parameter encapsulates the allocation information, from when value write started
     * and until value write was committed. It should not be used for other purposes, just transferred
     * between writeValueStart (return parameter) to writeValueCommit (input parameter)
     */
    final EntrySet.ValueBuffer newValue;

    /*-----------------------------------------------------------
     * Result Context
     *-----------------------------------------------------------*/

    final Result result;

    /*-----------------------------------------------------------
     * Temporary Context
     *-----------------------------------------------------------*/

    final EntrySet.KeyBuffer tempKey;
    final EntrySet.ValueBuffer tempValue;

    ThreadContext(int threadIndex, ValueUtils valueOperator) {
        this.threadIndex = threadIndex;
        this.key = new EntrySet.KeyBuffer();
        this.value = new EntrySet.ValueBuffer(valueOperator.getHeaderSize());
        this.newValue = new EntrySet.ValueBuffer(valueOperator.getHeaderSize());
        this.result = new Result();
        this.tempKey = new EntrySet.KeyBuffer();
        this.tempValue = new EntrySet.ValueBuffer(valueOperator.getHeaderSize());
        invalidate();
    }

    void invalidate() {
        entryIndex = EntrySet.INVALID_ENTRY_INDEX;
        key.invalidate();
        value.invalidate();
        newValue.invalidate();
        result.invalidate();
        valueState = ValueState.UNKNOWN;
        // No need to invalidate the temporary buffers
    }

    boolean isKeyValid() {
        return key.isValid();
    }

    boolean isValueValid() {
        return valueState.ordinal() >= ValueState.VALID.ordinal();
    }

    /**
     * Checks whether the version in the given lookUp is negative
     * (which means deleted) OR INVALID. Additionally check the off-heap deleted bit.
     * We can not proceed on entry with negative version,
     * it is first needs to be changed to invalid, then any other value reference (with version)
     * can be assigned to this entry (same key).
     */
    boolean isDeleteValueFinishNeeded() {
        return valueState == ValueState.MARKED_DELETED_NOT_FINALIZED;
    }

    /*
     * When value is connected to entry, first the value reference is CASed to the new one and after
     * the value version is set to the new one (written off-heap). Inside entry, when value reference
     * is invalid its version can only be invalid (0) or negative. When value reference is valid and
     * its version is either invalid (0) or negative, the insertion or deletion of the entry wasn't
     * accomplished, and needs to be accomplished.
     * */
    boolean isValueLinkNeeded() {
        return valueState == ValueState.VALID_INSERT_NOT_FINALIZED;
    }
}
