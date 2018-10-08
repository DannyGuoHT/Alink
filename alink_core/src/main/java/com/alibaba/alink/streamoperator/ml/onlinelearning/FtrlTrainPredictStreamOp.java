package com.alibaba.alink.streamoperator.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.SparseVector;
import com.alibaba.alink.common.matrix.Tensor;
import com.alibaba.alink.common.ml.LinearModel;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import com.alibaba.alink.streamoperator.StreamOperator;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

import static java.lang.Thread.sleep;

/**
 * *
 *
 *
 * this algorithm receive two streams : training sample stream and predicting sample stream.
 * using the training samples to update the model and using the updating model to predict the
 * predicting sample stream.
 *
 */
public class FtrlTrainPredictStreamOp extends StreamOperator {

    DirectReader.BatchStreamConnector connector = null;
    final private static String LONG = "LONG";
    final private static String DOUBLE = "DOUBLE";
    final private static String FLOAT = "FLOAT";
    final private static String BIGINT = "BIGINT";
    final private static String INTEGER = "INTEGER";

    public FtrlTrainPredictStreamOp() {
        super(null);
    }

    /**
     * constructor.
     *
     * @param model             the initial model of the algorithm.
     * @param tensorColName     tensor column name.
     * @param labelColName      label column name.
     * @param predResultColName predict result column name.
     * @param alpha
     * @param beta
     * @param l1
     * @param l2
     */
    public FtrlTrainPredictStreamOp(BatchOperator model, String tensorColName, String labelColName,
                                    String predResultColName, double alpha, double beta,
                                    double l1, double l2) throws Exception {
        this(model, new AlinkParameter()
            .put(ParamName.tensorColName, tensorColName)
            .put(ParamName.labelColName, labelColName)
            .put(ParamName.predResultColName, predResultColName)
            .put("alpha", alpha)
            .put("beta", beta)
            .put("l1", l1)
            .put("l2", l2)
        );
    }

    /**
     * constructor.
     *
     * @param params parameters of the algorithm.
     */
    public FtrlTrainPredictStreamOp(AlinkParameter params) throws Exception {
        this(null, params);
    }

    /**
     * constructor.
     *
     * @param model  the initial model of the algorithm.
     * @param params parameters of the algorithm.
     */
    public FtrlTrainPredictStreamOp(BatchOperator model, AlinkParameter params) throws Exception {
        super(params);

        if (model != null) {
            connector = new DirectReader().collect(model);
        }
    }

    /**
     * set the parameter of alpha.
     *
     * @param alpha the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setAlpha(double alpha){
        params.putIgnoreNull("alpha", alpha);
        return this;
    }

    /**
     * set the parameter of beta.
     *
     * @param beta the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setBeta(double beta){
        params.putIgnoreNull("beta", beta);
        return this;
    }

    /**
     * set the parameter of l1.
     *
     * @param l1 the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setL1(double l1){
        params.putIgnoreNull("l1", l1);
        return this;
    }

    /**
     * set the parameter of l2.
     *
     * @param l2 the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setL2(double l2){
        params.putIgnoreNull("l2", l2);
        return this;
    }

    /**
     * set the parameter of sparseFeatureDim.
     *
     * @param dim the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setSparseFeatureDim(long dim){
        params.putIgnoreNull(ParamName.sparseFeatureDim, dim);
        return this;
    }

    /**
     * set the parameter of HasInterceptItem.
     *
     * @param val the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setHasInterceptItem(boolean val){
        params.putIgnoreNull(ParamName.hasInterceptItem, val);
        return this;
    }

    /**
     * set the parameter of maxWaitTime.
     *
     * @param val the parameter of Ftrl.
     * @return this.
     */
    public FtrlTrainPredictStreamOp setMaxWaitTime(double val){
        params.putIgnoreNull("maxWaitTime", val);
        return this;
    }

    /**
     * set label column name
     *
     * @param value label column name
     * @return this
     */

    public FtrlTrainPredictStreamOp setLabelColName(String value) {
        params.putIgnoreNull(ParamName.labelColName, value);
        return this;
    }

    /**
     * set tensor column name
     *
     * @param value tensor column name
     * @return this
     */
    public FtrlTrainPredictStreamOp setTensorColName(String value) {
        params.putIgnoreNull(ParamName.tensorColName, value);
        return this;
    }

    /**
     * set predict result column name
     * @param value predict result column name
     * @return this
     */
    public FtrlTrainPredictStreamOp setPredResultColName(String value) {
        params.putIgnoreNull(ParamName.predResultColName, value);
        return this;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new RuntimeException("Need 2 inputs!");
    }

    @Override
    public StreamOperator linkFrom(List<StreamOperator> ins) {
        assert (ins.size() == 2);

        double alpha = this.params.getDoubleOrDefault("alpha", 0.1);
        double beta = this.params.getDoubleOrDefault("beta", 1.0);
        double l1 = this.params.getDoubleOrDefault("l1", this.params.getDoubleOrDefault("l1", 1.0));
        double l2 = this.params.getDoubleOrDefault("l2", this.params.getDoubleOrDefault("l2", 1.0));
        long featureSize = this.params.getLong(ParamName.sparseFeatureDim);
        boolean hasInterceptItem = this.params.getBoolOrDefault(ParamName.hasInterceptItem, true);

        /**
         * if the time without samples received, the algorithm finish and return.
         */
        long maxWaitTime = params.getLongOrDefault("maxWaitTime", Long.MAX_VALUE);

        String tensorName = params.getString(ParamName.tensorColName);
        String labelName = params.getString(ParamName.labelColName);
        String predResultColName = params.getString(ParamName.predResultColName);

        int tensorTrainIdx = TableUtil.findIndexFromName(ins.get(0).getColNames(), tensorName);
        int tensorPredictIdx = TableUtil.findIndexFromName(ins.get(1).getColNames(), tensorName);
        int labelIdx = TableUtil.findIndexFromName(ins.get(0).getColNames(), labelName);

        TypeInformation labelType = ins.get(1).getColTypes()[labelIdx];

        /**
         * merge two stream (train and pred stream) to one:
         */
        final ConnectedStreams<Row, Row> costream = ins.get(0).getDataStream().connect(ins.get(1).getDataStream());

        /**
         * Tuple3 : streamDataId, sampleLabel (train  2, pred 1), row
         */
        DataStream<Tuple3<Long, Integer, Row>>
            stream = costream.map(new RichCoMapFunction<Row, Row, Tuple3<Long, Integer, Row>>() {
            private long counter;
            private int parallelism;

            @Override
            public void open(Configuration parameters) throws Exception {
                parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
                counter = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public Tuple3<Long, Integer, Row> map1(Row row) throws Exception {
                Tuple3<Long, Integer, Row> ret = Tuple3.of(counter, 1, row);
                counter += parallelism;
                sleep(1);
                return ret;
            }

            @Override
            public Tuple3<Long, Integer, Row> map2(Row row) throws Exception {
                Tuple3<Long, Integer, Row> ret = Tuple3.of(counter, -1, row);
                counter += parallelism;
                sleep(1);
                return ret;
            }
        });

        int parallelism = AlinkSession.getStreamExecutionEnvironment().getParallelism();

        /**
         * Tuple2 : partitionId, Tuple3<streamDataId, trainOrPred, row>
         */
        DataStream<Tuple2<Integer, Tuple3<Long, Integer, Row>>>
            input = stream.flatMap(new BrocastAppendKey(parallelism))
            .partitionCustom(new CustomBlockPartitioner(), 0);

        /* Tuple5 : sampleID, partitionID, isTrainOrPred, row, y */
        IterativeStream.ConnectedIterativeStreams<Tuple2<Integer, Tuple3<Long, Integer, Row>>, Tuple5<Long, Integer,
            Integer, Row, Double>>
            iteration = input.iterate(maxWaitTime)
            .withFeedbackType(TypeInformation.of(new TypeHint<Tuple5<Long, Integer, Integer, Row, Double>>() {
            }));

        /* if feedback: update the coef; else do w*x+b */
        DataStream iterativeBody = iteration.flatMap(new CalcTask(connector, tensorTrainIdx,
            tensorPredictIdx, labelIdx, alpha, beta, l1, l2, featureSize,
            hasInterceptItem))
            .keyBy(0)
            .flatMap(new ReduceTask(parallelism))
            .partitionCustom(new CustomBlockPartitioner(), 1);

        DataStream<Tuple5<Long, Integer, Integer, Row, Double>>
            result = iterativeBody.filter(new FilterFunction<Tuple5<Long, Integer, Integer, Row, Double>>() {
            @Override
            public boolean filter(Tuple5<Long, Integer, Integer, Row, Double> t3) throws Exception {
                /* if equals 0 then feedback */
                return (t3.f2 == 0);
            }
        });

        iteration.closeWith(result);

        DataStream<Row> output = iterativeBody.filter(
            new FilterFunction<Tuple5<Long, Integer, Integer, Row, Double>>() {
                @Override
                public boolean filter(Tuple5<Long, Integer, Integer, Row, Double> value) throws Exception {
                    /* if equals -2, then output */
                    return value.f2 == -2;
                }
            }).map(new WriteData(labelType));

        TableSchema schema = new TableSchema(
            ArrayUtil.arrayMerge(ins.get(1).getColNames(), predResultColName),
            ArrayUtil.arrayMerge(ins.get(1).getColTypes(), labelType));

        this.setTable(output, schema);
        return this;
    }

    public static class WriteData implements MapFunction<Tuple5<Long, Integer, Integer, Row, Double>, Row> {
        private String type;

        public WriteData(TypeInformation type) {
            this.type = JdbcTypeConverter.getSqlType(type).toUpperCase();
        }

        @Override
        public Row map(Tuple5<Long, Integer, Integer, Row, Double> value) throws Exception {
            Row ret = new Row(value.f3.getArity() + 1);
            for (int i = 0; i < value.f3.getArity(); ++i) {
                ret.setField(i, value.f3.getField(i));
            }

            if (this.type.equalsIgnoreCase(LONG) || this.type.equalsIgnoreCase(BIGINT)) {
                Long val = (1 / (1 + Math.exp(-value.f4))) > 0.5 ? 1L : 0L;
                ret.setField(value.f3.getArity(), val);
            } else if (this.type.equalsIgnoreCase(INTEGER)) {
                int val = (1 / (1 + Math.exp(-value.f4))) > 0.5 ? 1 : 0;
                ret.setField(value.f3.getArity(), val);
            } else if (this.type.equalsIgnoreCase(DOUBLE)) {
                double val = (1 / (1 + Math.exp(-value.f4))) > 0.5 ? 1.0 : 0.0;
                ret.setField(value.f3.getArity(), val);
            } else if (this.type.equalsIgnoreCase(FLOAT)) {
                float val = (1 / (1 + Math.exp(-value.f4))) > 0.5 ? 1.0F : 0.0F;
                ret.setField(value.f3.getArity(), val);
            }
            return ret;
        }
    }

    public static class BrocastAppendKey
        implements FlatMapFunction<Tuple3<Long, Integer, Row>, Tuple2<Integer, Tuple3<Long, Integer, Row>>> {
        int partitionNum;

        public BrocastAppendKey(int partitionNum) {
            this.partitionNum = partitionNum;
        }

        @Override
        public void flatMap(Tuple3<Long, Integer, Row> value,
                            Collector<Tuple2<Integer, Tuple3<Long, Integer, Row>>> out) throws Exception {
            for (int i = 0; i < partitionNum; ++i) {
                out.collect(Tuple2.of(i, value));
            }
        }
    }

    public static class CalcTask extends RichCoFlatMapFunction<Tuple2<Integer, Tuple3<Long, Integer, Row>>,
        Tuple5<Long, Integer, Integer, Row, Double>, Tuple5<Long, Integer, Integer, Row, Double>> {
        private DirectReader.BatchStreamConnector connector;

        transient private double[] coef;
        transient private double[] nParam;
        transient private double[] zParam;

        private int tensorTrainIdx;
        private int tensorPredictIdx;
        private int labelIdx;
        private long startIdx;
        private long endIdx;
        private long weightSize;

        private double alpha = 0.1;
        private double beta = 1;
        private double l1 = 1;
        private double l2 = 1;

        private boolean hasInterceptItem;
        private long featureSize;

        public CalcTask(DirectReader.BatchStreamConnector connector, int tensorTrainIdx, int tensorPredictIdx,
                        int labelIdx, double alpha, double beta, double l1, double l2, long feaatureSize,
                        boolean hasInterceptItem) {
            this.connector = connector;
            this.tensorTrainIdx = tensorTrainIdx;
            this.tensorPredictIdx = tensorPredictIdx;
            this.alpha = alpha;
            this.beta = beta;
            this.l1 = l1;
            this.l2 = l2;
            this.labelIdx = labelIdx;
            this.featureSize = feaatureSize;
            this.hasInterceptItem = hasInterceptItem;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            LinearModel model = new LinearModel();
            int numWorkers = getRuntimeContext().getNumberOfParallelSubtasks();
            int workerId = getRuntimeContext().getIndexOfThisSubtask();
            Random random = new Random(1);
            if (connector != null) {
                /* read init model */
                List<Row> modelRows = connector.getBuffer();
                model.load(modelRows);
                weightSize = model.coefVector.size();
            } else {
                /* give a model with all coef value as 1.0 */
                weightSize = hasInterceptItem ? featureSize + 1 : featureSize;
            }

            if (weightSize < numWorkers) {
                throw new RuntimeException("feature Size is smaller than num workers");
            }
            int averNum = (int)weightSize / numWorkers;
            if (workerId < numWorkers - 1) {
                coef = new double[averNum];

                nParam = new double[averNum];
                zParam = new double[averNum];

                startIdx = averNum * workerId;
                endIdx = averNum * (workerId + 1);
                for (int i = averNum * workerId; i < averNum * (workerId + 1); ++i) {
                    coef[(int)(i - startIdx)] = (connector != null) ? model.coefVector.get(i) : random.nextDouble();
                }
            } else {
                coef = new double[(int)(weightSize - averNum * (numWorkers - 1))];

                nParam = new double[(int)(weightSize - averNum * (numWorkers - 1))];
                zParam = new double[(int)(weightSize - averNum * (numWorkers - 1))];
                startIdx = averNum * workerId;
                endIdx = weightSize;
                for (int i = averNum * workerId; i < weightSize; ++i) {
                    coef[(int)(i - startIdx)] = (connector != null) ? model.coefVector.get(i) : random.nextDouble();
                }
            }
        }

        @Override
        public void flatMap1(Tuple2<Integer, Tuple3<Long, Integer, Row>> value,
                             Collector<Tuple5<Long, Integer, Integer, Row, Double>> out) throws Exception {
            Row row = value.f1.f2;
            Long sampleId = value.f1.f0;
            Integer judge;

            String tensor;
            if (value.f1.f1 == 1) {
                tensor = (String)row.getField(tensorTrainIdx);
                judge = 0;
            } else {
                tensor = (String)row.getField(tensorPredictIdx);
                judge = -2;
            }
            SparseVector vec = Tensor.parse(tensor).toSparseVector();
            vec.setSize((int)featureSize);

            if (hasInterceptItem) {
                vec = vec.prefix(1.0);
            }
            double y = 0.0;
            int[] indices = vec.getIndices();

            for (int i = 0; i < indices.length; ++i) {
                if (indices[i] >= startIdx && indices[i] < endIdx) {
                    y += vec.values[i] * coef[(int)(indices[i] - startIdx)];
                }
            }
            out.collect(Tuple5.of(sampleId, -1, judge, row, y));
        }

        @Override
        public void flatMap2(Tuple5<Long, Integer, Integer, Row, Double> value,
                             Collector<Tuple5<Long, Integer, Integer, Row, Double>> out) throws Exception {
            double p = value.f4;
            Row row = value.f3;

            String tensor = (String)row.getField(tensorPredictIdx);

            SparseVector vec = Tensor.parse(tensor).toSparseVector();
            vec.setSize((int)featureSize);

            if (hasInterceptItem) {
                vec = vec.prefix(1.0);
            }
            int[] indices = vec.getIndices();

            /* eta */
            p = 1 / (1 + Math.exp(-p));

            double label = Double.valueOf(row.getField(labelIdx).toString());

            for (int i = 0; i < indices.length; ++i) {
                int idx = indices[i];
                if (idx >= startIdx && idx < endIdx) {
                    /* update zParam nParam */
                    int id = (int)(idx - startIdx);
                    double g = (p - label) * vec.getValues()[i];
                    double sigma = (Math.sqrt(nParam[id] + g * g) - Math.sqrt(nParam[id])) / alpha;
                    zParam[id] += g - sigma * coef[id];
                    nParam[id] += g * g;

                    /* update model coef */
                    if (Math.abs(zParam[id]) <= l1) {
                        coef[id] = 0.0;
                    } else {
                        coef[id] = ((zParam[id] < 0 ? -1 : 1) * l1 - zParam[id])
                            / ((beta + Math.sqrt(nParam[id]) / alpha + l2));
                    }
                }
            }
        }
    }

    public static class ReduceTask extends
        RichFlatMapFunction<Tuple5<Long, Integer, Integer, Row, Double>, Tuple5<Long, Integer, Integer, Row, Double>> {
        private int parallelism;

        private Map<Long, List<Object>> buffer;
        private Random rand = new Random(2018);

        public ReduceTask(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            buffer = new HashMap<>(0);
        }

        @Override
        public void flatMap(Tuple5<Long, Integer, Integer, Row, Double> value,
                            Collector<Tuple5<Long, Integer, Integer, Row, Double>> out) throws Exception {
            List<Object> val = buffer.get(value.f0);

            if (val == null) {
                val = new ArrayList<>();
                val.add(value);
                buffer.put(value.f0, val);
            } else {
                val.add(value);
            }
            Date nowTime = new Date();

            if (val.size() == parallelism) {
                double y = 0.0;
                for (Object t5 : val) {
                    Tuple5<Long, Integer, Integer, Row, Double> tmp = (Tuple5<Long, Integer, Integer, Row, Double>)t5;
                    y += tmp.f4;
                }

                for (int i = 0; i < parallelism; ++i) {
                    Tuple5<Long, Integer, Integer, Row, Double> ret
                        = (Tuple5<Long, Integer, Integer, Row, Double>)val.get(i);
                    ret.f4 = y;
                    ret.f1 = i;
                    if (value.f2 == -2) {
                        ret.f1 = rand.nextInt(parallelism);
                    }
                    out.collect(ret);
                    if (value.f2 == -2) {
                        break;

                    }
                }
                buffer.remove(value.f0);
            } else if (val.size() > parallelism) {
                throw new Exception(
                    "val.size > parallelism. val.size(): " + val.size() + ", parallelism: " + parallelism);
            }
        }
    }

    private static class CustomBlockPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }
}
