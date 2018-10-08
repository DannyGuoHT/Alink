package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.matrix.DenseMatrix;

/**
 * Naive bayes model info.
 *
 */
public class NaiveBayesProbInfo implements AlinkSerializable {
    /* labels */
    public Object [] labels = null;
    /* pi array */
    public double [] piArray = null;
    /* the probability matrix */
    public DenseMatrix theta;
}
