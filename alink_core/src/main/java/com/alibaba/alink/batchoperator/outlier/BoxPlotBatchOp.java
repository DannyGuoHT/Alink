package com.alibaba.alink.batchoperator.outlier;
/*
 * An implementation of Box Plot model train algorithm by Hu, guangyue
 * For more information of Box Plot,refer to https://en.wikipedia.org/wiki/Box_plot
 */

import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.statistics.QuantileBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.MLModel;
import com.alibaba.alink.common.outlier.BoxPlotModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.alibaba.alink.common.AlinkSession.gson;


public class BoxPlotBatchOp extends BatchOperator {

    private String task = null;

    /**
     * Constructor for BoxPlotBatchOp.
     *
     * @param params ALink parameters for this BatchOp
     *               The only parameter used is "featureColNames"
     */
    public BoxPlotBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * Default Constructor for BoxPlotBatchOp
     */
    public BoxPlotBatchOp() {
        super(null);
    }

    /**
     * Constructor for BoxPlotBatchOp.
     *
     * @param featureColNames ALink parameters for this BatchOp
     *                        The only parameter used is "featureColNames"
     */
    public BoxPlotBatchOp(String[] featureColNames) {
        super(null);
        setFeatureColNames(featureColNames);
        this.task = "outlier_box_plot";
    }

    /**
     * Set parameter featureColNames.
     *
     * @param featureColNames selected col names for Box Plot BatchOp
     */
    public BoxPlotBatchOp setFeatureColNames(String[] featureColNames) {
        putParamValue(ParamName.featureColNames, featureColNames);
        return this;
    }
    /*
     * Overide the method LinkFrom
     */

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        return trainBoxPlotModel(in);

    }

    /**
     * Train for Box Plot model.
     *
     * @param in input data batchoperator
     * @return the Box Plot model for predicting numeric value data
     */
    private BatchOperator trainBoxPlotModel(BatchOperator in) {

        if (null == params.getStringArray(ParamName.featureColNames)) {
            putParamValue(ParamName.featureColNames, in.getColNames());
        }
        String[] featureColNames = params.getStringArray(ParamName.featureColNames);
        if (params.size() < 1) {
            throw new RuntimeException("ColNames error");
        }
        //Get all the selected feature column indexes
        final int[] selectedColIndices = new int[featureColNames.length];
        for (int i = 0; i < selectedColIndices.length; i++) {
            selectedColIndices[i] = TableUtil.findIndexFromName(in.getColNames(), featureColNames[i]);
        }
        final String[] featureNames = featureColNames.clone();
        final String feature0 = featureNames[0];
        try {
            //Get four percentiles  via QuantileBatchOp
            BatchOperator featurePercentileData = in.linkTo(
                new QuantileBatchOp()
                    .setSelectedColName(featureNames[0])
                    .setQuantileNum(4)
                    .setRoundMode(QuantileBatchOp.RoundModeEnum.ROUND.name()));
            DataSet <Row> first = featurePercentileData.select(featureNames[0])
                .getDataSet()
                .reduceGroup(new BoxPlotGroupReduceFunction(feature0)).setParallelism(1);
            for (int i = 1; i < featureNames.length; i++) {
                final String feature = featureNames[i];
                featurePercentileData = in.linkTo(
                    new QuantileBatchOp()
                        .setSelectedColName(feature)
                        .setQuantileNum(4)
                        .setRoundMode(QuantileBatchOp.RoundModeEnum.ROUND.name()));
                first = first.union(featurePercentileData.select(feature)
                    .getDataSet()
                    .reduceGroup(new BoxPlotGroupReduceFunction(feature)).setParallelism(1));

            }
            first = first.mapPartition(new MapPartitionFunction <Row, Row>() {
                @Override
                public void mapPartition(Iterable <Row> iterable, Collector <Row> collector) throws Exception {
                    HashMap <String, HashMap <String, Double>> data = new HashMap <String, HashMap <String, Double>>();
                    for (Row row : iterable) {
                        String featureName = row.getField(0).toString();
                        String jsonString = row.getField(1).toString();
                        HashMap <String, Double> maps = gson.fromJson(jsonString, HashMap.class);
                        data.put(featureName, maps);
                    }

                    BoxPlotModel model = new BoxPlotModel("BoxPlotModel", featureNames, data);
                    //Save BoxPlotModel
                    List <Row> rows = model.save();
                    for (Row row : rows) {
                        collector.collect(row);
                    }
                }
            }).setParallelism(1);
            //Set table schema
            table = RowTypeDataSet.toTable(first,
                MLModel.getModelSchemaWithType(org.apache.flink.api.common.typeinfo.Types.DOUBLE));
            return this;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public BatchOperator linkFrom(List <BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }
}
