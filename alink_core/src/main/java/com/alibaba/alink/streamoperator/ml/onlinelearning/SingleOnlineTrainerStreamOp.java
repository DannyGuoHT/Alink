package com.alibaba.alink.streamoperator.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamDefaultValue;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.ml.LinearCoefInfo;
import com.alibaba.alink.common.ml.LinearModel;
import com.alibaba.alink.common.ml.LinearModelPredictor;
import com.alibaba.alink.common.ml.onlinelearning.SingleOnlineTrainer;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.concurrent.TimeUnit;


public abstract class SingleOnlineTrainerStreamOp extends StreamOperator {

    private final BatchOperator model;
    private int timeInterval;

    public SingleOnlineTrainerStreamOp(BatchOperator model, Class trainerClass, AlinkParameter params) {
        super(params);
        this.params.put(AlinkPredictor.CLASS_CANONICAL_NAME, trainerClass.getCanonicalName());
        this.model = model;
        this.timeInterval = super.params.getIntegerOrDefault(ParamName.timeInterval, ParamDefaultValue.timeInterval);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new RuntimeException("Need 2 inputs!");
    }

    @Override
    public StreamOperator linkFrom(List<StreamOperator> ins) {
        if (null == ins || ins.size() != 2) {
            throw new RuntimeException("Need 2 inputs!");
        }

        try {
            // for direct read
            DirectReader.BatchStreamConnector modelConnector = new DirectReader().collect(model);

            DataStream<LinearCoefInfo> model = ins.get(0).getDataStream()
                    .timeWindowAll(Time.of(this.timeInterval, TimeUnit.SECONDS))
                    .apply(new OnlineTrainer4AllWindow(modelConnector, TableUtil.toSchemaJson(ins.get(1).getSchema()), this.params))
                    .setParallelism(1);

            DataStream<Row> prediction = ins.get(1).getDataStream()
                    .connect(model)
                    .flatMap(new PredictProcess(modelConnector, TableUtil.toSchemaJson(this.model.getSchema()), TableUtil
                        .toSchemaJson(ins.get(1).getSchema()), this.params))
                    .setParallelism(1);

            this.table = RowTypeDataStream.toTable(prediction,
                    new LinearModelPredictor(this.model.getSchema(), ins.get(1).getSchema(), params).getResultSchema());

        } catch (Exception ex) {
            throw new RuntimeException(ex.toString());
        }

        return this;
    }

    public static class OnlineTrainer4AllWindow extends RichAllWindowFunction<Row, LinearCoefInfo, TimeWindow> {

        private DirectReader.BatchStreamConnector modelConnector = null;
        private SingleOnlineTrainer trainer = null;
        private List<Row> modelRows;
        private String dataSchemaJson;
        private AlinkParameter params;

        public OnlineTrainer4AllWindow(DirectReader.BatchStreamConnector modelConnector, String dataSchemaJson, AlinkParameter params) throws Exception {
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
            this.modelConnector = modelConnector;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (params.contains(AlinkPredictor.CLASS_CANONICAL_NAME)) {
                modelRows = new DirectReader().directRead(modelConnector);
                String trainerClassName = params.getString(AlinkPredictor.CLASS_CANONICAL_NAME);
                if (trainerClassName.startsWith("com.alibaba.alink")) {
                    Class trainerClass = Class.forName(trainerClassName);
                    this.trainer = (SingleOnlineTrainer) trainerClass
                            .getConstructor(List.class, TableSchema.class, AlinkParameter.class)
                            .newInstance(modelRows, TableUtil.fromSchemaJson(dataSchemaJson), params);
                }
            } else {
                throw new RuntimeException("Params not contain " + AlinkPredictor.CLASS_CANONICAL_NAME);
            }
        }

        @Override
        public void apply(TimeWindow window, Iterable<Row> values, Collector<LinearCoefInfo> out) throws Exception {
            this.trainer.train(values);
            out.collect(this.trainer.getCoefInfo());
        }

    }

    public static class PredictProcess extends RichCoFlatMapFunction<Row, LinearCoefInfo, Row> {

        private DirectReader.BatchStreamConnector modelConnector = null;
        private LinearModel linearModel;
        private LinearModelPredictor predictor = null;
        private List<Row> modelRows;
        private String modelSchemaJson;
        private String dataSchemaJson;
        private AlinkParameter params;

        public PredictProcess(DirectReader.BatchStreamConnector modelConnector, String modelSchemaJson, String dataSchemaJson, AlinkParameter params) {
            this.modelSchemaJson = modelSchemaJson;
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
            this.modelConnector = modelConnector;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            modelRows = new DirectReader().directRead(modelConnector);
            this.linearModel = new LinearModel();
            this.linearModel.load(modelRows);
            this.predictor = new LinearModelPredictor(TableUtil.fromSchemaJson(modelSchemaJson), TableUtil.fromSchemaJson(dataSchemaJson), this.params);
            this.predictor.loadModel(this.linearModel);
        }

        @Override
        public void flatMap1(Row row, Collector<Row> collector) throws Exception {
            collector.collect(this.predictor.predict(row));
        }

        @Override
        public void flatMap2(LinearCoefInfo coefInfo, Collector<Row> collector) throws Exception {
            this.linearModel.setCoefInfo(coefInfo);
            this.predictor.loadModel(this.linearModel);
        }
    }
}
