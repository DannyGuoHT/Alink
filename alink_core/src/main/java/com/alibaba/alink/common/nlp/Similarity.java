package com.alibaba.alink.common.nlp;

import java.io.Serializable;

/**
 * Interface for different similarity method.
 */
public interface Similarity extends Serializable {
    /**
     * @param left
     * @param right
     * @param <T> support String or Vector<String>
     * @return the similarity
     */
    <T> double similarity(T left, T right);

    /**
     * @param left
     * @param right
     * @param <T> support String or Vector<String>
     * @return the distance
     */
    <T> double distance(T left, T right);
}
