package com.alibaba.alink.common.matrix.string;


public abstract class StringVector {

    public StringVector() {
    }

    public abstract int size();

    public abstract String get(int i);

    public abstract void set(int i, String val);

    public abstract void add(int i, String val);

    public abstract StringVectorIterator iterator();


}
