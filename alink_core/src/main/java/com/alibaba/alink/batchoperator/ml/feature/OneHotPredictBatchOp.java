package com.alibaba.alink.batchoperator.ml.feature;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.feature.OneHotModelPredictor;

/**
 * *
 * A one-hot batch operator that maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 *
 */
public class OneHotPredictBatchOp extends AlinkPredictBatchOp {

    /**
     * constructor.
     */
    public OneHotPredictBatchOp() {
        super(OneHotModelPredictor.class, null);
    }

    /**
     * constructor.
     *
     * @param keepColNames the column names that will be kept in the output table.
     * @param predColName  predict result column name of output table.
     */
    public OneHotPredictBatchOp(String predColName, String[] keepColNames) {
        this(new AlinkParameter()
            .put(ParamName.outputColName, predColName)
            .put(ParamName.keepColNames, keepColNames)
        );
    }

    /**
     * constructor.
     *
     * @param params parameter set.
     */
    public OneHotPredictBatchOp(AlinkParameter params) {
        super(OneHotModelPredictor.class, params);
    }

    /**
     * set keep column names
     *
     * @param value keep column names
     * @return this
     */
    public OneHotPredictBatchOp setKeepColNames(String[] value) {
        putParamValue(ParamName.keepColNames, value);
        return this;
    }

    /**
     * set tensor column name
     *
     * @param value tensor column name
     * @return this
     */
    public OneHotPredictBatchOp setOutputColName(String value) {
        putParamValue(ParamName.outputColName, value);
        return this;
    }
}
