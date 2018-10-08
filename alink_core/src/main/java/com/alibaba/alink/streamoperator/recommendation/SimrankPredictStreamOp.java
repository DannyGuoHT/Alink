package com.alibaba.alink.streamoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.recommendation.SimrankModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

/**
 * input parameters:
 * -# predResultColName: required
 * -# keepColNames: optional
 */
public class SimrankPredictStreamOp extends AlinkPredictStreamOp {

    public SimrankPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, SimrankModelPredictor.class, params);
    }
}
