package com.oath.oak;

/**
 * As opposed to attached buffers, detached buffer can be returned to the user and may be stored for future use.
 * It is used in zero-copy API for:
 *   (1) get operations
 *   (2) iterations
 *   (3) stream-iterations, where we reuse the same detached buffer, i.e., it refers to different internal
 *       buffers as we iterate the map.
 **/
public interface OakDetachedBuffer extends OakReadBuffer {

    /**
     * Perform a transformation on the inner ByteBuffer atomically.
     *
     * @param transformer The function to apply on the ByteBuffer
     * @return The return value of the transform
     */
    <T> T transform(OakTransformer<T> transformer);

}
