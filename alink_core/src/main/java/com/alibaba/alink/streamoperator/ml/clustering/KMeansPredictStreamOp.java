package com.alibaba.alink.streamoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.clustering.KMeansModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

/**
 * <code>
 *     KMeansPredictStreamOp kMeansPredictStreamOp = new KMeansPredictStreamOp(kMeansBatchOp.getSideOutput(0))
 *      .setKeepColNames(new String[]{"id"})
 *      .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
 *     kMeansPredictStreamOp.linkFrom(predictTableStream).print();
 *     StreamOperator.execute();
 * </code>
 *
 */

public class KMeansPredictStreamOp extends AlinkPredictStreamOp {

    /**
     * default constructor
     *
     * @param model train from kMeansBatchOp
     */
    public KMeansPredictStreamOp(BatchOperator model) {
        this(model, new AlinkParameter());
    }

    /**
     * constructor
     *
     * @param model             train from kMeansBatchOp
     * @param keepColNames      forward cols from data
     * @param predResultColName predict result col name
     */
    public KMeansPredictStreamOp(BatchOperator model,
                                 String[] keepColNames,
                                 String predResultColName) {
        this(model);
        setKeepColNames(keepColNames);
        setPredResultColName(predResultColName);
    }

    public KMeansPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, KMeansModelPredictor.class, params);
    }

    /**
     * set keep col names which forward from data it is optional, default is all col of dats
     *
     * @param keepColNames keep col names
     * @return
     */
    public KMeansPredictStreamOp setKeepColNames(String[] keepColNames) {
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        return this;
    }

    /**
     * set predict  result col name, which is the col name of predict result. it is required.
     *
     * @param predResultColName predict col name
     * @return
     */
    public KMeansPredictStreamOp setPredResultColName(String predResultColName) {
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
        return this;
    }
}
