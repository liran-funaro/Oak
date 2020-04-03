package com.oath.oak;

/**
 * This detached buffer allows reuse of the same object and is used in:
 *   (1) Oak's stream iterators, where we reuse the same detached buffer, i.e., it refers to different internal
 *       buffers as we iterate the map.
 *   (2) normal iterations without reusing.
 *
 * Unlike other ephemeral objects, even if this references a value, it does not have to acquire a read lock
 * before each access since it can only be used without other concurrent writes in the background.
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
