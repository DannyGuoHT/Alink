package com.alibaba.alink.streamoperator.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.onlinelearning.PerceptronOnlineTrainer;


public class PerceptronSingleTrainerStreamOp extends SingleOnlineTrainerStreamOp {

    public PerceptronSingleTrainerStreamOp(BatchOperator model, String[] featureColNames, String labelColName, String predResultColName) throws Exception {
        this(model, new AlinkParameter()
                .put(ParamName.featureColNames, featureColNames)
                .put(ParamName.labelColName, labelColName)
                .put(ParamName.predResultColName, predResultColName)
        );
    }

    public PerceptronSingleTrainerStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, PerceptronOnlineTrainer.class, params);
    }

}
