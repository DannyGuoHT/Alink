package com.alibaba.alink.batchoperator.utils;


import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;


public abstract class AlinkPredictBatchOp extends BatchOperator {

    protected AlinkPredictBatchOp(Class predictorClass, AlinkParameter params) {
        super(params);
        if (null != predictorClass) {
            this.params.put(AlinkPredictor.CLASS_CANONICAL_NAME, predictorClass.getCanonicalName());
        }
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new RuntimeException("Need 2 inputs!");
    }

    @Override
    public BatchOperator linkFrom(List <BatchOperator> ins) {
        if (null == ins || ins.size() != 2) {
            throw new RuntimeException("Only support 2 inputs!");
        }

        AlinkModel.assertModelSchema(ins.get(0).getSchema());

        try {

            DataSet <Row> modelrows = ins.get(0).getDataSet();
            DataSet <Row> predRows = ins.get(1).getDataSet()
                    .map(new AlinkModelPredict(
                        TableUtil.toSchemaJson(ins.get(0).getSchema()), TableUtil.toSchemaJson(ins.get(1).getSchema()), this.params))
                    .withBroadcastSet(modelrows, "broadcastModelTable");

            this.table = RowTypeDataSet.toTable(
                    predRows,
                    AlinkPredictorFactory.create(ins.get(0).getSchema(), ins.get(1).getSchema(), this.params).getResultSchema()
//                    MLModelPredictor.getPredictResultSchema(ins.get(0).getSchema(), ins.get(1).getSchema(), this.params)
            );
            return this;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static class AlinkModelPredict extends RichMapFunction <Row, Row> implements Serializable {
        private AlinkPredictor predictor;
        private String modelSchemaJson;
        private String dataSchemaJson;
        private AlinkParameter params;

        public AlinkModelPredict(String modelSchemaJson, String dataSchemaJson, AlinkParameter params) {
            this.modelSchemaJson = modelSchemaJson;
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.predictor = AlinkPredictorFactory.create(
                    TableUtil.fromSchemaJson(modelSchemaJson),
                    TableUtil.fromSchemaJson(dataSchemaJson),
                    this.params,
                    getRuntimeContext(). <Row>getBroadcastVariable("broadcastModelTable"));
        }

        @Override
        public Row map(Row row) throws Exception {
            return predictor.predict(row);
        }
    }
}
