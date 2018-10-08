package com.alibaba.alink.common.matrix;


public abstract class Vector {

    public Vector() {
    }

    public abstract int size();

    public abstract double get(int i);

    public abstract void set(int i, double val);

    public abstract void add(int i, double val);

    public abstract double l1norm();

    public abstract double l2norm();

    public abstract double l2normSquare();

    public abstract Vector scale(double d);
    public abstract void scaleEqual(double d);

    public abstract VectorIterator iterator();
}
