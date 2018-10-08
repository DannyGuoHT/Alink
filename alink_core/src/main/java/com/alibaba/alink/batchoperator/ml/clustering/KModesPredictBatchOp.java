package com.alibaba.alink.batchoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.clustering.KModesModelPredictor;

/**
 *
 * <code>
 *     KModesPredictBatchOp kModesPredictBatchOp = new KModesPredictBatchOp()
 *      .setKeepColNames(new String[] {"id"})
 *      .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
 *     kModesPredictBatchOp.linkFrom(kModesBatchOp.getSideOutput(0), predictTableBatch).print();
 * </code>
 *
 */

public class KModesPredictBatchOp extends AlinkPredictBatchOp {

    /**
     * null constructor
     */
    public KModesPredictBatchOp() {
        this(null);
    }

    /**
     * constructor
     *
     * @param keepColNames      keep col names
     * @param predResultColName predict col name
     */
    public KModesPredictBatchOp(String[] keepColNames, String predResultColName) {
        this(new AlinkParameter()
            .put(ParamName.keepColNames, keepColNames)
            .put(ParamName.predResultColName, predResultColName)
        );
    }

    public KModesPredictBatchOp(AlinkParameter params) {
        super(KModesModelPredictor.class, params);
    }

    /**
     * set keep col names which forward from data it is optional, default is all col of dats
     *
     * @param keepColNames keep col names
     * @return
     */
    public KModesPredictBatchOp setKeepColNames(String[] keepColNames) {
        putParamValue(ParamName.keepColNames, keepColNames);
        return this;
    }

    /**
     * set predict  result col name, which is the col name of predict result. it is required.
     *
     * @param predResultColName predict col name
     * @return
     */
    public KModesPredictBatchOp setPredResultColName(String predResultColName) {
        putParamValue(ParamName.predResultColName, predResultColName);
        return this;
    }
}
