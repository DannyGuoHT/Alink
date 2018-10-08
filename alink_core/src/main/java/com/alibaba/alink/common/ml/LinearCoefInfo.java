package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.matrix.DenseVector;

public class LinearCoefInfo implements AlinkSerializable {
    public String[] featureColNames = null;
    public DenseVector coefVector = null;
    public DenseVector[] coefVectors = null;
}
