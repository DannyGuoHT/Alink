package com.alibaba.alink.streamoperator.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.outlier.SOSModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

/**
 * input parameters:
 * -# predResultColName: required
 * -# keepColNames: optional
 */

public class SosPredictStreamOp extends AlinkPredictStreamOp {

    public SosPredictStreamOp(BatchOperator model, String predResultColName) {
        this(model, new AlinkParameter().put("predResultColName", predResultColName));
    }

    public SosPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, SOSModelPredictor.class, params);
    }
}
