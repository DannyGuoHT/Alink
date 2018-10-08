package com.alibaba.alink.streamoperator.outlier;
/*
 * An implementation of Box Plot stream prediction algorithm by Hu, guangyue
 * For more information of Box Plot,refer to https://en.wikipedia.org/wiki/Box_plot
 */
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.outlier.BoxPlotModelPredictor;
import com.alibaba.alink.streamoperator.utils.AlinkPredictStreamOp;

public class BoxPlotPredictStreamOp extends AlinkPredictStreamOp {

    public BoxPlotPredictStreamOp(BatchOperator model, String predResultColName) {
        this(model, new AlinkParameter().put("predResultColName", predResultColName));
    }
    /**
     * Constructor for BoxPlotPredictStreamOp.
     *@param model ALink model for Box Plot
     * @param params ALink parameters for this BatchOp
     * The only parameter used is "predResultColName"
     */
    public BoxPlotPredictStreamOp(BatchOperator model, AlinkParameter params) {
        super(model, BoxPlotModelPredictor.class, params);
    }
}
