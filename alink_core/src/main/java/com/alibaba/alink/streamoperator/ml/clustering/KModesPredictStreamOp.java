package com.alibaba.alink.streamoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.clustering.KModesModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

/**
 * <code>
 *     KModesPredictStreamOp kModesPredictStreamOp = new KModesPredictStreamOp(kModesBatchOp.getSideOutput(0))
 *      .setKeepColNames(new String[]{"id"})
 *      .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
 *     kModesPredictStreamOp.linkFrom(predictTableStream).print();
 *     StreamOperator.execute();
 * </code>
 *
 */

public class KModesPredictStreamOp extends AlinkPredictStreamOp {

    /**
     * default constructor
     *
     * @param model train from kMeansBatchOp
     */
    public KModesPredictStreamOp(BatchOperator model) {
        this(model, new AlinkParameter());
    }

    /**
     * constructor
     *
     * @param model             train from kMeansBatchOp
     * @param keepColNames      forward cols from data
     * @param predResultColName predict result col name
     */
    public KModesPredictStreamOp(BatchOperator model,
                                 String[] keepColNames,
                                 String predResultColName) {
        this(model);
        setKeepColNames(keepColNames);
        setPredResultColName(predResultColName);
    }

    public KModesPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, KModesModelPredictor.class, params);
    }

    /**
     * set keep col names which forward from data it is optional, default is all col of dats
     *
     * @param keepColNames keep col names
     * @return
     */
    public KModesPredictStreamOp setKeepColNames(String[] keepColNames) {
        this.params.putIgnoreNull(ParamName.keepColNames, keepColNames);
        return this;
    }

    /**
     * set predict  result col name, which is the col name of predict result. it is required.
     *
     * @param predResultColName predict col name
     * @return
     */
    public KModesPredictStreamOp setPredResultColName(String predResultColName) {
        this.params.putIgnoreNull(ParamName.predResultColName, predResultColName);
        return this;
    }
}
