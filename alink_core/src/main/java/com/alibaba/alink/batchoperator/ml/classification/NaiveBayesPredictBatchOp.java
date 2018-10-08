package com.alibaba.alink.batchoperator.ml.classification;

import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.NaiveBayesModelPredictor;

/**
 * *
 *
 * Naive Bayes Predictor.
 *
 * we support the multinomial Naive Bayes and multinomial NB model, a probabilistic learning method.
 * here, feature values of train table must be nonnegative.
 *
 * details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 *
 */
public class NaiveBayesPredictBatchOp extends AlinkPredictBatchOp {

    public NaiveBayesPredictBatchOp() {
        super(NaiveBayesModelPredictor.class, null);
    }

    /**
     * constructor.
     * @param predResultColName predict result column name of output table.
     */
    public NaiveBayesPredictBatchOp(String predResultColName) {
        this(null, predResultColName);
    }

    /**
     * constructor.
     * @param tensorColName tensor column name of input table.
     * @param predResultColName predict result column name of output table.
     */
    public NaiveBayesPredictBatchOp(String tensorColName, String predResultColName) {
        this(tensorColName, predResultColName, null);
    }

    /**
     * constructor.
     * @param tensorColName tensor column name of input table.
     * @param predResultColName predict result column name of output table.
     * @param predDetailColName detail info of results.
     */
    public NaiveBayesPredictBatchOp(String tensorColName, String predResultColName, String predDetailColName) {
        this(tensorColName, null, predResultColName, predDetailColName);
    }

    /**
     * constructor.
     * @param tensorColName tensor column name of input table.
     * @param predResultColName predict result column name of output table.
     * @param predDetailColName detail info of results.
     * @param keepColNames the column names of input table that will be kept in output table.
     */
    public NaiveBayesPredictBatchOp(String tensorColName, String[] keepColNames, String predResultColName,
                                    String predDetailColName) {
        super(null, null);
        this.params.putIgnoreNull(ParamName.tensorColName, tensorColName);
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
        this.params.putIgnoreNull(ParamName.predDetailColName, predDetailColName);
    }

    /**
     * constructor.
     * @param params the parameters set.
     */
    public NaiveBayesPredictBatchOp(AlinkParameter params) {
        super(NaiveBayesModelPredictor.class, params);
    }

    /**
     * set keep column names
     * @param value keep column names
     * @return this
     */
    public NaiveBayesPredictBatchOp setKeepColNames(String[] value) {
        putParamValue(ParamName.keepColNames, value);
        return this;
    }

    /**
     * set tensor column name
     * @param value tensor column name
     * @return this
     */
    public NaiveBayesPredictBatchOp setTensorColName(String value) {
        putParamValue(ParamName.tensorColName, value);
        return this;
    }

    /**
     * set predict result column name
     * @param value predict result column name
     * @return this
     */
    public NaiveBayesPredictBatchOp setPredResultColName(String value) {
        putParamValue(ParamName.predResultColName, value);
        return this;
    }

    /**
     * set predict detail result column name
     * @param value the detail info column name
     * @return this
     */
    public NaiveBayesPredictBatchOp setPredDetailColName(String value) {
        putParamValue(ParamName.predDetailColName, value);
        return this;
    }
}
