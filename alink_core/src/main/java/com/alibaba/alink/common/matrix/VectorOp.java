package com.alibaba.alink.common.matrix;

import java.util.List;


public class VectorOp {

    public VectorOp() {
    }

    public static Vector plus(Vector vec1, Vector vec2) {
        if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
            return ((DenseVector) vec1).plus((DenseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec1).plus((SparseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
            return ((SparseVector) vec1).plus((DenseVector) vec2);
        } else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec2).plus((DenseVector) vec1);
        } else {
            throw new RuntimeException("Not implemented yet!");
        }
    }

    public static Vector minus(Vector vec1, Vector vec2) {
        if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
            return ((DenseVector) vec1).minus((DenseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec1).minus((SparseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
            return ((SparseVector) vec1).minus((DenseVector) vec2);
        } else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec2).scale(-1.0).plus((DenseVector) vec1);
        } else {
            throw new RuntimeException("Not implemented yet!");
        }
    }

    public static Vector scale(Vector vec, double d) {
        return vec.scale(d);
    }

    public static double dot(Vector vec1, Vector vec2) {
        if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
            return ((DenseVector) vec1).dot((DenseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec1).dot((SparseVector) vec2);
        } else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
            return ((SparseVector) vec1).dot((DenseVector) vec2);
        } else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
            return ((SparseVector) vec2).dot((DenseVector) vec1);
        } else {
            throw new RuntimeException("Not implemented yet!");
        }
    }

    public static double l1norm(Vector vec) {
        return vec.l1norm();
    }

    public static double l2norm(Vector vec) {
        return vec.l2norm();
    }

    public static DenseVector toDenseVector(Vector vec) {
        if (vec instanceof DenseVector) {
            return new DenseVector((DenseVector) vec);
        } else if (vec instanceof SparseVector) {
            return ((SparseVector) vec).toDenseVector();
        } else {
            throw new RuntimeException("unsupported vector type.");
        }
    }

    public static DenseVector mean(List<DenseVector> vectors) {
        if (null != vectors && vectors.size() > 0) {
            int dim = vectors.get(0).size();
            DenseVector r = DenseVector.zeros(dim);
            for (DenseVector vector : vectors) {
                r.plusEqual(vector);
            }
            r.scaleEqual(1.0 / vectors.size());
            return r;
        }
        return null;
    }

}
