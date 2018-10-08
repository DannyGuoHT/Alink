package com.alibaba.alink.batchoperator.outlier;
/*
 * An implementation of Box Plot batch prediction algorithm by Hu, guangyue
 * For more information of Box Plot,refer to https://en.wikipedia.org/wiki/Box_plot
 */
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.utils.AlinkPredictBatchOp;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.outlier.BoxPlotModelPredictor;

public class BoxPlotPredictBatchOp extends AlinkPredictBatchOp {
    /**
//     * default constructor
//     */
    public BoxPlotPredictBatchOp() {
        this(null);
    }
    /**
     * Constructor for BoxPlotPredictBatchOp.
     *
     * @param predResultColName  result column name for prediction
     * The only parameter used is "predResultColName"
//     */
//    public BoxPlotPredictBatchOp(String predResultColName) {
//        //this(new AlinkParameter().put(ParamName.predResultColName, predResultColName));
//        setPredResultColName(predResultColName);
//        this(params);
//    }

    /**
     * Constructor for BoxPlotPredictBatchOp.
     *
     * @param params ALink parameters for this BatchOp
     * The only parameter used is "predResultColName"
     */
    public BoxPlotPredictBatchOp(AlinkParameter params) {
        super(BoxPlotModelPredictor.class, params);
    }

    /**
     * set predict  result col name, which is the col name of predict result.
     * it is required.
     *
     * @param predResultColName predict col name
     * @return
     */
    public BoxPlotPredictBatchOp setPredResultColName(String predResultColName){
        putParamValue(ParamName.predResultColName, predResultColName);
        return this;
    }
}
