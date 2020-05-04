package com.oath.oak;

public class ValueBuffer extends OakAttachedReadBuffer {
    protected long reference;

    public ValueBuffer(int headerSize) {
        super(headerSize);
    }

    public ValueBuffer(ValueUtils valueOperator) {
        super(valueOperator.getHeaderSize());
    }

    public ValueBuffer(ValueBuffer value) {
        super(value);
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
