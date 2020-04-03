package com.oath.oak;

/**
 * A reference is composed of 3 parameters: block ID, offset and length.
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 */
class ReferenceCodec {
    final public static long INVALID_REFERENCE = 0;

    final int offsetBitSize;
    final int lengthBitSize;
    final int blockBitSize;

    final int lengthShift;
    final int blockShift;

    final long offsetMask;
    final long lengthMask;
    final long blockMask;

    public ReferenceCodec(int offsetBitSize, int lengthBitSize, int blockBitSize) {
        this.offsetBitSize = offsetBitSize;
        this.lengthBitSize = lengthBitSize;
        this.blockBitSize = blockBitSize;

        lengthShift = offsetBitSize;
        blockShift = lengthShift + lengthBitSize;

        offsetMask = mask(offsetBitSize);
        lengthMask = mask(lengthBitSize);
        blockMask = mask(blockBitSize);
    }

    static long mask(int size) {
        return (1L << size) - 1L;
    }

    static boolean isValidReference(long reference) {
        return reference != INVALID_REFERENCE;
    }

    /*
    In the implementation of encode/decode methods, we make two assumptions that
    are true in all modern architectures:
      (1) CPU level parallelism: independent instructions will be executed simultaneously.
      (2) Shift complexity: shift (>>) op takes a fixed number of cycles
                            (its complexity is independent of the shift size).
     This means that the three mask+shift operations will be executed (and finished) simultaneously.
     */

    public long encode(final Slice s) {
        long offsetPart = ((long) s.getAllocOffset()) & offsetMask;
        long lengthPart = (((long) s.getAllocLength()) & lengthMask) << lengthShift;
        long blockPart = (((long) s.getAllocBlockID()) & blockMask) << blockShift;
        return offsetPart | lengthPart | blockPart;
    }

    public boolean decode(final Slice s, final long reference) {
        if (!isValidReference(reference)) {
            s.invalidate();
            return false;
        }

        int offset = (int) (reference & offsetMask);
        int length = (int) ((reference >>> lengthShift) & lengthMask);
        int blockId = (int) ((reference >>> blockShift) & blockMask);
        s.update(blockId, offset, length);
        return true;
    }
}
