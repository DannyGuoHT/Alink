package com.alibaba.alink.batchoperator.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.outlier.SOSModelPredictor;

/**
 * input parameters:
 * -# predResultColName: required
 * -# keepColNames: optional
 */

public class SosPredictBatchOp extends AlinkPredictBatchOp {

    public SosPredictBatchOp(String predResultColName) {
        this(new AlinkParameter().put(ParamName.predResultColName, predResultColName));
    }

    public SosPredictBatchOp(AlinkParameter params) {
        super(SOSModelPredictor.class, params);
    }
}
