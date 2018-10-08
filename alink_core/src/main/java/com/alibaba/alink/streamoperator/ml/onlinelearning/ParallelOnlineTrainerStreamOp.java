package com.alibaba.alink.streamoperator.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.ml.LinearModel;
import com.alibaba.alink.common.ml.LinearModelPredictor;
import com.alibaba.alink.common.ml.onlinelearning.SingleOnlineTrainer;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class ParallelOnlineTrainerStreamOp extends StreamOperator {

    private final BatchOperator model;
    private long maxWaitTime;

    public ParallelOnlineTrainerStreamOp(BatchOperator model, Class trainerClass, AlinkParameter params) throws Exception {
        super(params);
        this.params.put(AlinkPredictor.CLASS_CANONICAL_NAME, trainerClass.getCanonicalName());
        this.model = model;
        this.maxWaitTime = params.getLongOrDefault("maxWaitTime", Long.MAX_VALUE);
    }

    private enum MessageType implements Serializable {
        ToTrainerWeights, // weights, stepNo
        ToReducerWeights, // weights, sampleCount, stepNo, trainerTaskId
        ToPredictorWeights,
        TrainerQueuedSampleCount, // count, trainerTaskId
    }

    public static class Message extends Tuple2<MessageType, Object> {
        public Message() {
        }

        public Message(MessageType messageType, Object object) {
            super.f0 = messageType;
            super.f1 = object;
        }

        public MessageType getMessageType() {
            return f0;
        }

        public Object getValue() {
            return f1;
        }
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

        if (null == this.model) {
            throw new RuntimeException("Need an initial model!");
        }

        AlinkModel.assertModelSchema(this.model.getSchema());

        try {

            final int numTrainerWorkers = ins.get(0).getDataStream().getExecutionEnvironment().getParallelism();

            List<Row> modelRows = this.model.collect();
            String modelSchema = TableUtil.toSchemaJson(this.model.getSchema());
            String dataSchema = TableUtil.toSchemaJson(ins.get(1).getSchema());

            IterativeStream.ConnectedIterativeStreams<Row, Tuple2<double[], Long>> iteration =
                    ins.get(0).getDataStream()
                            .iterate(this.maxWaitTime)
                            .withFeedbackType(TypeInformation.of(new TypeHint<Tuple2<double[], Long>>() {
                            }));

            DataStream<Message> toWeightReducer = iteration
                    .flatMap(new TrainingOperator(modelRows, dataSchema, this.params));

            DataStream<Message> outputOfWeightReducer = toWeightReducer
                    .flatMap(new ReduceWeightsOperator(modelRows, numTrainerWorkers, this.params))
                    .setParallelism(1);

            DataStream<DenseVector> toPredictor = outputOfWeightReducer
                    .flatMap(new FlatMapFunction<Message, DenseVector>() {
                        @Override
                        public void flatMap(Message value, Collector<DenseVector> out) throws Exception {
                            if (value.getMessageType().equals(MessageType.ToPredictorWeights))
                                out.collect((DenseVector) value.getValue());
                        }
                    })
                    .broadcast();

            DataStream<Tuple2<double[], Long>> toTrainer = outputOfWeightReducer
                    .filter(new FilterFunction<Message>() {
                        @Override
                        public boolean filter(Message value) throws Exception {
                            return value.getMessageType().equals(MessageType.ToTrainerWeights);
                        }
                    })
                    .map(new MapFunction<Message, Tuple2<double[], Long>>() {
                        @Override
                        public Tuple2<double[], Long> map(Message value) throws Exception {
                            Tuple2<DenseVector, Long> message = (Tuple2<DenseVector, Long>) value.getValue();
                            double[] values = new double[message.f0.size()];
                            for (int i = 0; i < values.length; i++) {
                                values[i] = message.f0.get(i);
                            }
                            return new Tuple2<>(values, message.f1);
                        }
                    })
                    .broadcast();

            iteration.closeWith(toTrainer);

            DataStream<Row> prediction = ins.get(1).getDataStream()
                    .connect(toPredictor)
                    .flatMap(new PredictOperator(modelRows, modelSchema, dataSchema, this.params))
                    .setParallelism(1);

            this.table = RowTypeDataStream.toTable(prediction,
                    new LinearModelPredictor(this.model.getSchema(), ins.get(1).getSchema(), params).getResultSchema());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.toString());
        }

        return this;
    }

    private static class TrainingOperator extends RichCoFlatMapFunction<Row, Tuple2<double[], Long>, Message> {
        private List<Row> modelRows = null;
        private SingleOnlineTrainer trainer = null;
        private String dataSchemaJson;
        private AlinkParameter params;

        private Queue<Row> queue = null;
        private final int maxQueueSize = 100000;

        private int taskId;
        private DenseVector weights = null;
        private long stepNo = -1L;

        public TrainingOperator(List<Row> modelRows, String dataSchemaJson, AlinkParameter params) {
            this.modelRows = modelRows;
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
        }

        private void train(List<Row> samples, Collector<Message> out) {
            this.trainer.setWeights(weights);
            this.trainer.train(samples);
            // tuple4: weights, sampleCount, stepNo, trainerTaskId
            out.collect(new Message(MessageType.ToReducerWeights,
                    new Tuple4<>(trainer.getCoefInfo().coefVector, samples.size(), stepNo, getRuntimeContext().getIndexOfThisSubtask())));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.taskId = getRuntimeContext().getIndexOfThisSubtask();
            this.queue = new LinkedList<>();

            if (!params.contains(AlinkPredictor.CLASS_CANONICAL_NAME)) {
                throw new RuntimeException("Params not contain " + AlinkPredictor.CLASS_CANONICAL_NAME);
            }

            String trainerClassName = params.getString(AlinkPredictor.CLASS_CANONICAL_NAME);
            if (trainerClassName.startsWith("com.alibaba.alink")) {
                Class trainerClass = Class.forName(trainerClassName);
                this.trainer = (SingleOnlineTrainer) trainerClass
                        .getConstructor(List.class, TableSchema.class, AlinkParameter.class)
                        .newInstance(modelRows, TableUtil.fromSchemaJson(dataSchemaJson), params);
            }
        }

        @Override
        public void flatMap1(Row sample, Collector<Message> out) throws Exception {
            int size = queue.size();

            queue.add(sample);
            if (size + 1 > maxQueueSize)
                queue.poll();
            else {
                size++;
                if (size % 10 == 0)
                    out.collect(new Message(MessageType.TrainerQueuedSampleCount, new Tuple2<>(taskId, size)));
            }
        }

        @Override
        public void flatMap2(Tuple2<double[], Long> in, Collector<Message> out) throws Exception {
            this.weights = new DenseVector(in.f0);
            this.stepNo = in.f1;


            int n = queue.size();
            List<Row> samples = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                samples.add(queue.poll());
            }

            // the arrival of message from WeightReducer signals training
            System.out.println("trainBatch " + taskId + " training at step " + stepNo);
            train(samples, out);
        }
    }

    private static class ReduceWeightsOperator extends RichFlatMapFunction<Message, Message> {
        private List<Row> modelRows;
        private int numTrainerTasks;
        private AlinkParameter params;

        private DenseVector weights;
        private DenseVector[] receivedWeights = null;
        private int[] receivedCounts = null;
        private long[] receivedStepNos = null;
        private int[] trainerQueuedSampleCount;
        private int D; // dimension of weights

        long stepNo = -1L;
        long timeStamp = 0L;

        public ReduceWeightsOperator(List<Row> modelRows, int numTrainerTasks, AlinkParameter params) {
            this.modelRows = modelRows;
            this.numTrainerTasks = numTrainerTasks;
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getRuntimeContext().getNumberOfParallelSubtasks() != 1)
                throw new RuntimeException("parallelism not one");

            LinearModel linearModel = new LinearModel();
            linearModel.load(modelRows);
            this.weights = linearModel.getCoefVector();
            this.D = this.weights.size();

            receivedWeights = new DenseVector[numTrainerTasks];
            receivedCounts = new int[numTrainerTasks];
            receivedStepNos = new long[numTrainerTasks];
            Arrays.fill(receivedStepNos, -1L);
            trainerQueuedSampleCount = new int[numTrainerTasks];
            Arrays.fill(trainerQueuedSampleCount, 0);
            timeStamp = System.currentTimeMillis();
        }

        @Override
        public void flatMap(Message in, Collector<Message> out) throws Exception {
            if (in.getMessageType().equals(MessageType.ToReducerWeights)) {
                // tuple4: weights, sampleCount, stepNo, trainerTaskId
                Tuple4<DenseVector, Integer, Long, Integer> incomingWeights = (Tuple4<DenseVector, Integer, Long, Integer>) in.getValue();
                int taskId = incomingWeights.f3;
                receivedWeights[taskId] = incomingWeights.f0;
                receivedCounts[taskId] = incomingWeights.f1;
                receivedStepNos[taskId] = incomingWeights.f2;
                System.out.println("weight reducer " + getRuntimeContext().getIndexOfThisSubtask() + " "
                        + "receive trainBatch " + taskId + "'s new weights of step " + incomingWeights.f2);
            } else if (in.getMessageType().equals(MessageType.TrainerQueuedSampleCount)) {
                Tuple2<Integer, Integer> queuedSampleCount = (Tuple2<Integer, Integer>)in.getValue();
                int taskId = queuedSampleCount.f0;
                trainerQueuedSampleCount[taskId] = queuedSampleCount.f1;

                if (updateCriteria()) {
                    System.out.println("weight reducer: updating weights at step " + stepNo);
                    updateWeights();
                    afterUpdate();
                    Message toTrainer = new Message(MessageType.ToTrainerWeights, new Tuple2<>(weights, stepNo));
                    out.collect(toTrainer);
                    Message toPredictor = new Message(MessageType.ToPredictorWeights, weights);
                    out.collect(toPredictor);
                }
            } else {
                throw new RuntimeException("unexpected message type " + in.getMessageType());
            }
        }


        private boolean updateCriteria() {
            final long timeIntervalThreshold = 3000; // milliseconds
            long deltaT = System.currentTimeMillis() - timeStamp;
            if (deltaT > timeIntervalThreshold)
                return true;

            final int trainerSamplesCountsThreshold = 1000;
            int totQueuedSamplesCount = 0;
            for (int i = 0; i < numTrainerTasks; i++) {
                totQueuedSamplesCount += trainerQueuedSampleCount[i];
            }
            if (totQueuedSamplesCount > trainerSamplesCountsThreshold)
                return true;

            return false;
        }

        private void updateWeights() {
            if (stepNo < 0) {
                return;
            }

            DenseVector sum = DenseVector.zeros(D);
            int cnt = 0;
            for (int i = 0; i < numTrainerTasks; i++) {
                assert (receivedStepNos[i] <= stepNo);
                if (receivedStepNos[i] < stepNo) {
                    continue; // drop stale data
                }

                cnt++;
                for (int j = 0; j < D; j++) {
                    sum.add(j, receivedWeights[i].get(j));
                }
            }
            if (cnt > 0) {
                sum.scale(1. / cnt); // TODO: weighted average ?
                weights = sum;
            }
        }

        private void afterUpdate() {
            Arrays.fill(trainerQueuedSampleCount, 0);
            timeStamp = System.currentTimeMillis();
            stepNo++;
        }
    }

    private static class PredictOperator extends RichCoFlatMapFunction<Row, DenseVector, Row> {
        private LinearModel linearModel;
        private LinearModelPredictor predictor = null;
        private List<Row> modelRows;
        private String modelSchemaJson;
        private String dataSchemaJson;
        private AlinkParameter params;

        public PredictOperator(List<Row> modelRows, String modelSchemaJson, String dataSchemaJson, AlinkParameter params) {
            this.modelRows = modelRows;
            this.modelSchemaJson = modelSchemaJson;
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
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
        public void flatMap2(DenseVector weights, Collector<Row> collector) throws Exception {
            System.out.println("predictor " + getRuntimeContext().getIndexOfThisSubtask() + " receive updated weights");
            this.linearModel.setCoefVector(weights);
            this.predictor.loadModel(this.linearModel);
        }
    }
}
