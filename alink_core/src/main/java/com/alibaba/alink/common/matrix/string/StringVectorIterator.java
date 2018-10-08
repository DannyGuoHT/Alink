package com.alibaba.alink.common.matrix.string;

/**
 * Usage:
 *
 * StringVector stringVector = ...;
 * StringVectorIterator iterator = stringVector.iterator();
 *
 * while(iterator.hasNext()) {
 *     int index = iterator.getIndex();
 *     String value = iterator.getValue();
 *     iterator.next();
 * }
 */
public interface StringVectorIterator {

    boolean hasNext();

    void next();

    int getIndex();

    String getValue();
}
