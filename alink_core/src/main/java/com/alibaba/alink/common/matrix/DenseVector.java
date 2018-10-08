package com.alibaba.alink.common.matrix;


import java.io.Serializable;
import java.util.Arrays;

public class DenseVector extends Vector implements Serializable {

    private double[] data;

    public DenseVector() {
    }

    public DenseVector(DenseVector other) {
        this.data = other.data.clone();
    }

    public DenseVector(int n) {
        this.data = new double[n];
        Arrays.fill(this.data, 0.);
    }

    public DenseVector(double[] data) {
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
    public double get(int i) {
        return data[i];
    }

    @Override
    public void set(int i, double d) {
        data[i] = d;
    }

    @Override
    public void add(int i, double d) {
        data[i] += d;
    }

    public static DenseVector ones(int n) {
        DenseVector r = new DenseVector(n);
        for (int i = 0; i < r.data.length; i++) {
            r.data[i] = 1.0;
        }
        return r;
    }

    public static DenseVector zeros(int n) {
        DenseVector r = new DenseVector(n);
        for (int i = 0; i < r.data.length; i++) {
            r.data[i] = 0.0;
        }
        return r;
    }

    @Override
    public double l1norm() {
        double d = 0;
        for (double t : data) d += Math.abs(t);
        return d;
    }

    @Override
    public double l2norm() {
        double d = 0;
        for (double t : data) d += t * t;
        return Math.sqrt(d);
    }

    @Override
    public double l2normSquare() {
        double d = 0;
        for (double t : data) d += t * t;
        return d;
    }

    public DenseVector minus(DenseVector other) {
        DenseVector r = new DenseVector(this.size());
        for (int i = 0; i < this.size(); i++) {
            r.data[i] = this.data[i] - other.data[i];
        }
        return r;
    }

    public DenseVector plus(DenseVector other) {
        DenseVector r = new DenseVector(this.size());
        for (int i = 0; i < this.size(); i++) {
            r.data[i] = this.data[i] + other.data[i];
        }
        return r;
    }

    @Override
    public DenseVector scale(double d) {
        DenseVector r = new DenseVector(this.data);
        for (int i = 0; i < this.size(); i++) {
            r.data[i] *= d;
        }
        return r;
    }

    public void setEqual(DenseVector other) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = other.data[i];
        }
    }

    public void minusEqual(DenseVector other) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = this.data[i] - other.data[i];
        }
        ;
    }

    public void plusEqual(DenseVector other) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = this.data[i] + other.data[i];
        }
    }

    public void plusScaleEqual(Vector vec, double val) {
        if (vec instanceof SparseVector) {
            SparseVector spvec = (SparseVector) vec;
            int[] indices = spvec.getIndices();
            double[] values = spvec.getValues();
            int size = indices.length;
            for (int i = 0; i < size; ++i) {
                this.add(indices[i], values[i] * val);
            }
        } else {
            double[] vecdata = ((DenseVector) vec).data;
            for (int i = 0; i < this.size(); i++) {
                this.data[i] += vecdata[i] * val;
            }
        }
    }

    public void minusEqual(DenseVector v1, DenseVector v2) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = v1.data[i] - v2.data[i];
        }
    }

    public void plusEqual(DenseVector v1, DenseVector v2) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = v1.data[i] + v2.data[i];
        }
    }

//    public void plusScaleEqual(DenseVector other, Double alpha) {
//        for (int i = 0; i < this.size(); i++) {
//            this.data[i] = this.data[i] + other.data[i] * alpha;
//        }
//    }

    public void minusScaleEqual(DenseVector other, Double alpha) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = this.data[i] - other.data[i] * alpha;
        }
    }

    public void minusEqual(double d) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] -= d;
        }
    }

    public void plusEqual(double d) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] += d;
        }
    }

    @Override
    public void scaleEqual(double d) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] *= d;
        }
    }

    public void scaleEqual(DenseVector other, double d) {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = other.data[i] * d;
        }
    }

    public double dot(DenseVector other) {
        double d = 0;
        for (int i = 0; i < this.size(); i++) {
            d += this.data[i] * other.data[i];
        }
        return d;
    }

    public DenseVector prefix(double d) {
        double[] newVec = new double[this.size() + 1];
        newVec[0] = d;
        for (int i = 0; i < this.size(); i++) {
            newVec[i + 1] = this.get(i);
        }
        return new DenseVector(newVec);
    }

    public DenseVector append(double d) {
        double[] newVec = new double[this.size() + 1];
        for (int i = 0; i < this.size(); i++) {
            newVec[i] = this.get(i);
        }
        newVec[this.size()] = d;
        return new DenseVector(newVec);
    }

    public DenseVector round() {
        DenseVector r = new DenseVector(this.size());
        for (int i = 0; i < this.size(); i++) {
            r.data[i] = Math.round(this.data[i]);
        }
        return r;
    }

    public void roundEqual() {
        for (int i = 0; i < this.size(); i++) {
            this.data[i] = Math.round(this.data[i]);
        }
    }

    public DenseMatrix outer() {
        return this.outer(this);
    }

    public DenseMatrix outer(DenseVector other) {
        int nrows = this.size();
        int ncols = other.size();
        double[][] A = new double[nrows][ncols];
        for (int i = 0; i < nrows; i++) {
            for (int j = 0; j < ncols; j++) {
                A[i][j] = this.data[i] * other.data[j];
            }
        }
        return new DenseMatrix(A);
    }

    public double[] toDoubleArray() {
        return this.data.clone();
    }

    private class DenseVectorVectorIterator implements VectorIterator {
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
            if (cursor >= data.length)
                throw new RuntimeException("iterator out of bound");
            return cursor;
        }

        @Override
        public double getValue() {
            if (cursor >= data.length)
                throw new RuntimeException("iterator out of bound");
            return data[cursor];
        }
    }

    @Override
    public VectorIterator iterator() {
        return new DenseVectorVectorIterator();
    }
}
