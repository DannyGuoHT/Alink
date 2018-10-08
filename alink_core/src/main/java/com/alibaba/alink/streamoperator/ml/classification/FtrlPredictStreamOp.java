package com.alibaba.alink.streamoperator.ml.classification;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;


public class FtrlPredictStreamOp extends AlinkPredictStreamOp {

    public FtrlPredictStreamOp(BatchOperator model, String predResultColName) {
        this(model, null, predResultColName);
    }

    public FtrlPredictStreamOp(BatchOperator model, String tensorColName, String predResultColName) {
        this(model, tensorColName, null, predResultColName);
    }

    public FtrlPredictStreamOp(BatchOperator model, String predResultColName, AlinkParameter params) {
        this(model, null, predResultColName, params);
    }

    public FtrlPredictStreamOp(BatchOperator model, String tensorColName, String predResultColName,
                               AlinkParameter params) {
        super(model, null, params);
        this.params.put(ParamName.tensorColName, tensorColName);
        this.params.put(ParamName.predResultColName, predResultColName);
    }

    public FtrlPredictStreamOp(BatchOperator model, String tensorColName, String[] keepColNames,
                               String predResultColName) {
        super(model, null, null);
        this.params.putIgnoreNull(ParamName.tensorColName, tensorColName);
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
    }

    public FtrlPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, null, params);
    }

}
