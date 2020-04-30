package com.oath.oak;

/**
 * Encapsulates the lookup information, from when entry (key) was
 * (or wasn't) found and until the operation was completed.
 */
class ThreadContext {

    /* The index of the thread that this context belongs to */
    final int threadIndex;

    /*-----------------------------------------------------------
     * Entry Context
     *-----------------------------------------------------------/

    /* The index of the key's entry in EntrySet */
    int entryIndex;

    /* key is used for easier access to the off-heap memory */
    final EntrySet.KeyBuffer key;

    /* The state of the value */
    EntrySet.ValueState valueState;

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
        valueState = EntrySet.ValueState.UNKNOWN;
        // No need to invalidate the temporary buffers
    }

    boolean isKeyValid() {
        return key.isValid();
    }

    boolean isValueValid() {
        return valueState.isValid();
    }
}
