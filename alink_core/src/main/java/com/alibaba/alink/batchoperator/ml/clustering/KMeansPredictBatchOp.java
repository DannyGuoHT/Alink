package com.alibaba.alink.batchoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.ml.clustering.KMeansModelPredictor;
import com.alibaba.alink.common.constants.ParamName;

/**
 *
 * <code>
 *     KMeansPredictBatchOp kMeansPredictBatchOp = new KMeansPredictBatchOp()
 *      .setKeepColNames(new String[] {"id"})
 *      .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
 *     kMeansPredictBatchOp.linkFrom(kMeansBatchOp.getSideOutput(0), predictTableBatch).print();
 * </code>
 *
 */

public class KMeansPredictBatchOp extends AlinkPredictBatchOp {

    /**
     * null constructor
     */
    public KMeansPredictBatchOp(){
        this(null);
    }

    /**
     * constructor
     *
     * @param keepColNames      keep col names
     * @param predResultColName predict col name
     */
    public KMeansPredictBatchOp(String[] keepColNames, String predResultColName) {
        this(new AlinkParameter()
            .put(ParamName.keepColNames, keepColNames)
            .put(ParamName.predResultColName, predResultColName)
        );
    }

    /**
     * constructor
     * @param params
     */
    public KMeansPredictBatchOp(AlinkParameter params) {
        super(KMeansModelPredictor.class, params);
    }

    /**
     * set keep col names which forward from data
     * it is optional, default is all col of dats
     *
     * @param keepColNames keep col names
     * @return
     */
    public KMeansPredictBatchOp setKeepColNames(String[] keepColNames) {
        putParamValue(ParamName.keepColNames, keepColNames);
        return this;
    }

    /**
     * set predict  result col name, which is the col name of predict result.
     * it is required.
     *
     * @param predResultColName predict col name
     * @return
     */
    public KMeansPredictBatchOp setPredResultColName(String predResultColName) {
        putParamValue(ParamName.predResultColName, predResultColName);
        return this;
    }
    
}
