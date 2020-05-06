package com.oath.oak;

/**
 * Encapsulates the entry context, from when entry (key) was
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
    final KeyBuffer key;

    /* The state of the value */
    EntrySet.ValueState valueState;

    /* value is used for easier access to the off-heap memory */
    final ValueBuffer value;

    /*-----------------------------------------------------------
     * Value Insertion Context
     *-----------------------------------------------------------*/

    /**
     * This parameter encapsulates the allocation information, from when value write started
     * and until value write was committed. It should not be used for other purposes, just transferred
     * between writeValueStart (return parameter) to writeValueCommit (input parameter)
     */
    final ValueBuffer newValue;

    /*-----------------------------------------------------------
     * Result Context
     *-----------------------------------------------------------*/

    final Result result;

    /*-----------------------------------------------------------
     * Temporary Context
     *-----------------------------------------------------------*/

    final KeyBuffer tempKey;
    final ValueBuffer tempValue;

    ThreadContext(int threadIndex, ValueUtils valueOperator) {
        this.threadIndex = threadIndex;
        this.key = new KeyBuffer();
        this.value = new ValueBuffer(valueOperator.getHeaderSize());
        this.newValue = new ValueBuffer(valueOperator.getHeaderSize());
        this.result = new Result();
        this.tempKey = new KeyBuffer();
        this.tempValue = new ValueBuffer(valueOperator.getHeaderSize());
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

    /**
     * We consider a key to be valid if the entry referred to a valid allocation.
     * @return does the entry have a valid key
     */
    boolean isKeyValid() {
        return key.isAllocated();
    }

    /**
     * See {@code ValueState.isValid()} for more details.
     * @return does the entry have a valid value
     */
    boolean isValueValid() {
        return valueState.isValid();
    }
}
