package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.recommendation.SimrankModelPredictor;

/**
 * input parameters:
 * -# predResultColName: required
 * -# keepColNames: optional
 *
 */
public class SimrankPredictBatchOp extends AlinkPredictBatchOp {

    public SimrankPredictBatchOp(AlinkParameter params) {
        super(SimrankModelPredictor.class, params);
    }
}