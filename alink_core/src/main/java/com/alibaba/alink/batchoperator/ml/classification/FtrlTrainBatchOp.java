package com.alibaba.alink.batchoperator.ml.classification;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.ml.OnlineLearningTrainBatchOp;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.onlinelearning.FtrlOnlineTrainer;

/**
 * *
 *
 */
public class FtrlTrainBatchOp extends OnlineLearningTrainBatchOp {

    public FtrlTrainBatchOp(String[] featureNames, String labelName) {
        this(featureNames, labelName, 0.1, 1, 1, 1);
    }

    public FtrlTrainBatchOp(String[] featureNames, String labelName, double alpha, double beta, double l1, double l2) {
        this(featureNames, labelName, alpha, beta, l1, l2, false, null);
    }

    public FtrlTrainBatchOp(String[] featureNames, String labelName, double alpha, double beta, double l1, double l2,
                            boolean isSparse, Integer sparseFeatureDim) {
        this(new AlinkParameter()
                .put(ParamName.featureColNames, featureNames)
                .put(ParamName.labelColName, labelName)
                .put(ParamName.isSparse, isSparse)
                .put(ParamName.sparseFeatureDim, sparseFeatureDim)
                .put("alpha", alpha)
                .put("beta", beta)
                .put("l1", l1)
                .put("l2", l2)
                .put(ParamName.hasInterceptItem, Boolean.FALSE)
        );
    }

    public FtrlTrainBatchOp(AlinkParameter params) {
        super(FtrlOnlineTrainer.class, params);
    }
}