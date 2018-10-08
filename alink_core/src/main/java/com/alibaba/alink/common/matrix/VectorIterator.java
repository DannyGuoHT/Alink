package com.alibaba.alink.common.matrix;

/**
 * Usage:
 *
 * Vector vector = ...;
 * VectorIterator iterator = vector.iterator();
 *
 * while(iterator.hasNext()) {
 *     int index = iterator.getIndex();
 *     double value = iterator.getValue();
 *     iterator.next();
 * }
 */
public interface VectorIterator {

    boolean hasNext();

    void next();

    int getIndex();

    double getValue();
}
