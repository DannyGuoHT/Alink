package com.alibaba.alink.streamoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.recommendation.ALSRatingModelPredictor;
import com.alibaba.alink.common.recommendation.ALSTopKModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

/**
 * input parameters:
 * -# task: optional, "rating" or "topk", default "topk", should agree with trainBatch
 * -# predResultColName: required
 * -# keepColNames: optional
 *
 */
public class AlsPredictStreamOp extends AlinkPredictStreamOp {

    public AlsPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, params.getStringOrDefault("task", "topk").equalsIgnoreCase("rating") ?
                ALSRatingModelPredictor.class : ALSTopKModelPredictor.class, params);
    }
}
