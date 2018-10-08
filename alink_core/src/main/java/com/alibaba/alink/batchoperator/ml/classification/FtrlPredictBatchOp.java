package com.alibaba.alink.batchoperator.ml.classification;

import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.LinearModelPredictor;

/**
 * *
 *
 */
public class FtrlPredictBatchOp extends AlinkPredictBatchOp {

    public FtrlPredictBatchOp(String predResultColName) {
        this(null, predResultColName);
    }

    public FtrlPredictBatchOp(String tensorColName, String predResultColName) {
        this(tensorColName, null, predResultColName);
    }

    public FtrlPredictBatchOp(String tensorColName, String[] keepColNames, String predResultColName) {
        super(null, null);
        this.params.putIgnoreNull(ParamName.tensorColName, tensorColName);
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
    }

    public FtrlPredictBatchOp(AlinkParameter params) {
        super(LinearModelPredictor.class, params);
    }

}
