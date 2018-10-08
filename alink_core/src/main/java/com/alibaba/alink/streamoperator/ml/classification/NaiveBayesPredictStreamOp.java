package com.alibaba.alink.streamoperator.ml.classification;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.NaiveBayesModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;


public class NaiveBayesPredictStreamOp extends AlinkPredictStreamOp {

    /**
     * constructor.
     * @param model the model.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model) {
        super(model, NaiveBayesModelPredictor.class, null);
    }

    /**
     * constructor.
     * @param model the model.
     * @param predResultColName predict result column name of output table.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String predResultColName) {
        this(model, null, predResultColName);
    }

    /**
     * constructor.
     * @param model the model.
     * @param tensorColName tensor column name of input table.
     * @param predResultColName predict result column name of output table.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String tensorColName, String predResultColName) {
        this(model, tensorColName, null, predResultColName, null);
    }

    /**
     * constructor.
     * @param model the model.
     * @param predResultColName predict result column name of output table.
     * @param params the parameter set.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String predResultColName, AlinkParameter params) {
        this(model, null, predResultColName, params);
    }

    /**
     * constructor.
     * @param model the model.
     * @param tensorColName tensor column name of input table.
     * @param predResultColName predict result column name of output table.
     * @param params the parameter set.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String tensorColName, String predResultColName,
                                     AlinkParameter params) {
        super(model, null, params);
        this.params.put(ParamName.tensorColName, tensorColName);
        this.params.put(ParamName.predResultColName, predResultColName);
    }

    /**
     * constructor.
     * @param model the model.
     * @param tensorColName tensor column name of input table.
     * @param keepColNames the column names that will be kept in the output table.
     * @param predResultColName predict result column name of output table.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String tensorColName, String[] keepColNames,
                                     String predResultColName) {
        this(model, tensorColName, keepColNames, predResultColName, null);
    }

    /**
     * constructor.
     * @param model the model.
     * @param tensorColName tensor column name of input table.
     * @param keepColNames the column names that will be kept in the output table.
     * @param predResultColName predict result column name of output table.
     * @param predDetailColName detail info of results.
     */
    public NaiveBayesPredictStreamOp(BatchOperator model, String tensorColName, String[] keepColNames,
                                     String predResultColName, String predDetailColName) {
        super(model, null, null);
        this.params.putIgnoreNull(ParamName.tensorColName, tensorColName);
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
        this.params.putIgnoreNull(ParamName.predDetailColName, predDetailColName);
    }

    public NaiveBayesPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, null, params);
    }


    /**
     * set keep column names
     * @param value keep column names
     * @return this
     */
    public NaiveBayesPredictStreamOp setKeepColNames(String[] value) {
        params.putIgnoreNull(ParamName.keepColNames, value);
        return this;
    }

    /**
     * set tensor column name
     * @param value tensor column name
     * @return this
     */
    public NaiveBayesPredictStreamOp setTensorColName(String value) {
        params.putIgnoreNull(ParamName.tensorColName, value);
        return this;
    }

    /**
     * set predict result column name
     * @param value predict result column name
     * @return this
     */
    public NaiveBayesPredictStreamOp setPredResultColName(String value) {
        params.putIgnoreNull(ParamName.predResultColName, value);
        return this;
    }

    /**
     * set predict detail result column name
     * @param value the detail info column name
     * @return this
     */
    public NaiveBayesPredictStreamOp setPredDetailColName(String value) {
        params.putIgnoreNull(ParamName.predDetailColName, value);
        return this;
    }

}
