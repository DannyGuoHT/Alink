package com.alibaba.alink.streamoperator.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.onlinelearning.FtrlOnlineTrainer;


public class FtrlSingleTrainerStreamOp extends SingleOnlineTrainerStreamOp {

    public FtrlSingleTrainerStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, FtrlOnlineTrainer.class, params);
    }

    public FtrlSingleTrainerStreamOp(BatchOperator model, String[] featureColNames, String labelColName, String predResultColName) {
        this(model, featureColNames, labelColName, predResultColName, 0.1, 1, 1, 1);
    }

    public FtrlSingleTrainerStreamOp(BatchOperator model, String[] featureColNames, String labelColName, String predResultColName, double alpha, double beta, double L1, double L2) {
        super(model, FtrlOnlineTrainer.class,
                new AlinkParameter()
                        .put(ParamName.featureColNames, featureColNames)
                        .put(ParamName.labelColName, labelColName)
                        .put(ParamName.predResultColName, predResultColName)
                        .put("alpha", alpha)
                        .put("beta", beta)
                        .put("L1", L1)
                        .put("L2", L2)
        );
    }

}
