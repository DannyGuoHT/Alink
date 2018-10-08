package com.alibaba.alink.common.matrix.string;

import java.io.Serializable;
import java.util.Arrays;

public class DenseStringVector extends StringVector implements Serializable {

    private String[] data;

    public DenseStringVector() {
    }

    public DenseStringVector(DenseStringVector other) {
        this.data = other.data.clone();
    }

    public DenseStringVector(int n) {
        this.data = new String[n];
        Arrays.fill(this.data, "");//empty
    }

    public DenseStringVector(String[] data) {
        this.data = data.clone();
    }

    @Override
    public String toString() {
        return "DenseVector{" +
            "vec=" + Arrays.toString(data) +
            '}';
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public String get(int i) {
        return data[i];
    }

    @Override
    public void set(int i, String d) {
        data[i] = d;
    }

    @Override
    public void add(int i, String d) {
        data[i] += d;
    }

    public DenseStringVector plus(DenseStringVector other) {
        DenseStringVector r = new DenseStringVector(this.size());
        for (int i = 0; i < this.size(); i++) {
            r.data[i] = this.data[i] + other.data[i];
        }
        return r;
    }

    public void setEqual(DenseStringVector other) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = other.data[i];
        }
    }

    public void plusEqual(DenseStringVector other) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = this.data[i] + other.data[i];
        }
    }

    private class DenseVectorVectorIterator implements StringVectorIterator {
        private int cursor = 0;

        @Override
        public boolean hasNext() {
            return cursor < data.length;
        }

        @Override
        public void next() {
            cursor++;
        }

        @Override
        public int getIndex() {
            if (cursor >= data.length) { throw new RuntimeException("iterator out of bound"); }
            return cursor;
        }

        @Override
        public String getValue() {
            if (cursor >= data.length) { throw new RuntimeException("iterator out of bound"); }
            return data[cursor];
        }
    }

    @Override
    public StringVectorIterator iterator() {
        return new DenseVectorVectorIterator();
    }
}
