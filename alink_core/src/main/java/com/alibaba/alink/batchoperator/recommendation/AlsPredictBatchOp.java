package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.recommendation.ALSRatingModelPredictor;
import com.alibaba.alink.common.recommendation.ALSTopKModelPredictor;

/**
 * input parameters:
 * -# task: optional, "rating" or "topk", default "topk", should agree with trainBatch
 * -# predResultColName: required
 * -# keepColNames: optional
 *
 */
public class AlsPredictBatchOp extends AlinkPredictBatchOp {

    public AlsPredictBatchOp(AlinkParameter params) {
        super(params.getStringOrDefault("task", "topk").equalsIgnoreCase("rating") ?
                ALSRatingModelPredictor.class : ALSTopKModelPredictor.class, params);
    }
}