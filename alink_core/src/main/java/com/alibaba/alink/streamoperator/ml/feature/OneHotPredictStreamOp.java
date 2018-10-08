package com.alibaba.alink.streamoperator.ml.feature;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.feature.OneHotModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;


public class OneHotPredictStreamOp extends AlinkPredictStreamOp {

    /**
     * constructor.
     * @param model the model.
     */
    public OneHotPredictStreamOp(BatchOperator model) {
        super(model, OneHotModelPredictor.class, null);
    }

    /**
     * constructor.
     * @param model the model.
     * @param keepColNames the column names that will be kept in the output table.
     * @param predColName predict result column name of output table.
     */
    public OneHotPredictStreamOp(BatchOperator model, String predColName, String[] keepColNames) {
        this(model, new AlinkParameter()
                .put(ParamName.outputColName, predColName)
                .put(ParamName.keepColNames, keepColNames)
        );
    }

    /**
     *
     * @param model the model.
     * @param params the parameter set.
     */
    public OneHotPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, OneHotModelPredictor.class, params);
    }

    /**
     * set keep column names
     * @param value keep column names
     * @return this
     */
    public OneHotPredictStreamOp setKeepColNames(String[] value) {
        params.putIgnoreNull(ParamName.keepColNames, value);
        return this;
    }

    /**
     * set tensor column name
     * @param value tensor column name
     * @return this
     */
    public OneHotPredictStreamOp setOutputColName(String value) {
        params.putIgnoreNull(ParamName.outputColName, value);
        return this;
    }
}
