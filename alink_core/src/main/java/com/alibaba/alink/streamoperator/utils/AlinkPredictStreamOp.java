package com.alibaba.alink.streamoperator.utils;

import java.io.Serializable;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.AlinkPredictorFactory;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.streamoperator.StreamOperator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;


public abstract class AlinkPredictStreamOp extends StreamOperator {

    private final BatchOperator model;

    protected AlinkPredictStreamOp(BatchOperator model, Class predictorClass, AlinkParameter params) {
        super(params);
        this.model = model;
        if (null != predictorClass) {
            this.params.put(AlinkPredictor.CLASS_CANONICAL_NAME, predictorClass.getCanonicalName());
        }
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {

        TableSchema modelSchema = this.model.getSchema();
        AlinkModel.assertModelSchema(modelSchema);

        try {
            DirectReader.BatchStreamConnector modelConnector = new DirectReader().collect(model);

            DataStream <Row> predRows = in.getDataStream()
                .map(new AlinkModelPredict(modelConnector, TableUtil.toSchemaJson(this.model.getSchema()), TableUtil
                    .toSchemaJson(in.getSchema()), this.params));

            this.table = RowTypeDataStream.toTable(
                predRows,
                AlinkPredictorFactory.create(modelSchema, in.getSchema(), this.params).getResultSchema()
            );

            return this;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static class AlinkModelPredict extends RichMapFunction <Row, Row> implements Serializable {
        private AlinkPredictor predictor = null;
        private DirectReader.BatchStreamConnector modelConnector;
        private String modelSchemaJson;
        private String dataSchemaJson;
        private AlinkParameter params;

        public AlinkModelPredict(DirectReader.BatchStreamConnector modelConnector, String modelSchemaJson,
                                 String dataSchemaJson, AlinkParameter params) {
            this.modelConnector = modelConnector;
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
                new DirectReader().directRead(modelConnector));
        }

        @Override
        public Row map(Row row) throws Exception {
            return predictor.predict(row);
        }
    }

}
