package com.alibaba.alink.streamoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ClusteringParamName;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.ml.clustering.DistanceType;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.ml.clustering.KMeansModel;
import com.alibaba.alink.common.recommendation.KeepColNamesManager;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class StreamingKMeansStreamOp extends StreamOperator {
    BatchOperator batchModel = null;
    private String predResultColName = null;

    /**
     * time interval for updating the model, in seconds
     */
    private long timeInterval;
    /**
     * in seconds
     */
    private long halfLife;
    private double decayFactor;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    private KeepColNamesManager keepColManager = null;

    public StreamingKMeansStreamOp(BatchOperator batchModel, AlinkParameter params) {
        super(params);
        initWithParams(params);
        this.batchModel = batchModel;
    }

    private void initWithParams(AlinkParameter params) {
        this.predResultColName = params.getString(ParamName.predResultColName);
        this.timeInterval = params.getInteger(ClusteringParamName.timeInterval);
        this.halfLife = params.getInteger(ClusteringParamName.halfLife);
        this.decayFactor = Math.pow(0.5, (double) timeInterval / (double) halfLife);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new RuntimeException("require two input stream operator.");
    }

    @Override
    public StreamOperator linkFrom(List<StreamOperator> ins) {
        if (ins.size() != 2) {
            throw new IllegalArgumentException("require 2 inputs");
        }
        return linkFrom(ins.get(0), ins.get(1));
    }

    /**
     * Update model with stream in1, predict for stream in2
     */
    @Override
    public StreamOperator linkFrom(StreamOperator in1, StreamOperator in2) {
        try {
            DataStream<Row> trainingData = in1.getDataStream();
            DataStream<Row> predictData = in2.getDataStream();

            this.keepColManager = new KeepColNamesManager(null, in2.getSchema(), new String[]{predResultColName}, params);
            TableSchema outputSchema = new TableSchema(
                    ArrayUtil.arrayMerge(keepColManager.getKeepColNames(), predResultColName),
                    ArrayUtil.arrayMerge(keepColManager.getKeepColTypes(), org.apache.flink.table.api.Types.LONG())
            );

            // for direct read
            DirectReader.BatchStreamConnector modelConnector = new DirectReader().collect(batchModel);

            // incremental train on every window of data
            DataStream<Model> streamModel = trainingData
                // tumbling process-/event- time window
                    .timeWindowAll(Time.of(this.timeInterval, this.timeUnit))
                    .apply(new UpdateModelOp(modelConnector, in1.getColNames(), decayFactor))
                    .setParallelism(1);

            // predict
            DataStream<Row> predictResult = predictData
                    .connect(streamModel)
                    .flatMap(new PredictOp(modelConnector, in2.getColNames(), keepColManager.getKeepColIndices()))
                    .setParallelism(1);

            this.table = RowTypeDataStream.toTable(predictResult, outputSchema);
            return this;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public static class Model implements Serializable {
        List<Tuple3<Long, Double, DenseVector>> centroids = null;
        int dim;
        int k;
        String[] featureColNames = null;
        DistanceType distanceType;

        public Model(List<Tuple3<Long, Double, DenseVector>> centroids, String[] featureColNames,DistanceType distanceType) {
            this.centroids = centroids;
            this.k = centroids.size();
            this.dim = featureColNames.length;
            this.featureColNames = featureColNames;
            this.distanceType = distanceType;
        }

        /**
         * Update the model given:
         * -# the original model (centroids with weights)
         * -# a batch of new samples
         * -# decayFactor
         */
        public void updateModel(List<DenseVector> samples, double decayFactor) {
            int count = samples.size();

            // sum of samples of each cluster
            DenseVector[] sum = new DenseVector[k];
            for (int i = 0; i < k; i++) {
                sum[i] = DenseVector.zeros(dim);
            }

            int[] clusterCount = new int[k];
            Arrays.fill(clusterCount, 0);

            // find nearest cluster to each sample
            for (int i = 0; i < count; i++) {
                int clusterId = (int) KMeansModel.findCluster(centroids, samples.get(i),this.distanceType);
                clusterCount[clusterId]++;
                sum[clusterId].plusEqual(samples.get(i));
            }

            // update cluster centers and weights
            double discount = decayFactor;

            for (int i = 0; i < centroids.size(); i++) {
                int id = centroids.get(i).f0.intValue();
                double w = centroids.get(i).f1;
                double nw = discount * w + (double) clusterCount[id];
                centroids.get(i).f1 = nw;

                if (clusterCount[id] > 0) {
                    for (int j = 0; j < dim; j++) {
                        double ov = centroids.get(i).f2.get(j);
                        double v = ov * discount * w + sum[id].get(j);
                        v /= nw;
                        centroids.get(i).f2.set(j, v);
                    }
                    if (DistanceType.HAMMING.equals(distanceType)) {
                        centroids.get(i).f2.roundEqual();
                    }
                }
            }
        }

        public long predict(DenseVector sample) {
            return KMeansModel.findCluster(this.centroids, sample,this.distanceType);
        }
    }

    private static Model initModel(DirectReader.BatchStreamConnector modelConnector) {
        try {
            List<Row> modelRows = new DirectReader().directRead(modelConnector);
            KMeansModel kMeansModel = new KMeansModel();
            kMeansModel.load(modelRows);

            String[] featureColNames = kMeansModel.getMeta().getStringArray(ParamName.featureColNames);
            List<Tuple3<Long, Double, DenseVector>> centroids = kMeansModel.getCentroids();
            DistanceType distanceType = kMeansModel.getDistanceType();

            Model initialModel = new Model(centroids, featureColNames, distanceType);
            return initialModel;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private static class UpdateModelOp extends RichAllWindowFunction<Row, Model, TimeWindow> {
        DirectReader.BatchStreamConnector modelConnector;
        private Model model = null;
        private int[] selectedColIndices = null;
        private double decayFactor;
        private String[] trainColNames;

        public UpdateModelOp(DirectReader.BatchStreamConnector modelConnector, String[] trainColNames, double decayFactor) {
            this.modelConnector = modelConnector;
            this.selectedColIndices = null;
            this.decayFactor = decayFactor;
            this.trainColNames = trainColNames;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.model = initModel(modelConnector);
            String[] featureColNames = model.featureColNames;
            selectedColIndices = new int[featureColNames.length];
            for (int i = 0; i < featureColNames.length; i++) {
                selectedColIndices[i] = TableUtil.findIndexFromName(trainColNames, featureColNames[i]);
                if (selectedColIndices[i] < 0) {
                    throw new IllegalArgumentException(
                        "can't find feature col name in train data: " + featureColNames[i]);
                }
            }
        }

        @Override
        public void apply(TimeWindow window, Iterable<Row> values, Collector<Model> out) throws Exception {
            List<DenseVector> samples = new ArrayList<>();

            Iterator<Row> rows = values.iterator();
            while (rows.hasNext()) {
                Row row = rows.next();
                int dim = this.selectedColIndices.length;
                DenseVector v = new DenseVector(dim);
                for (int i = 0; i < dim; i++) {
                    v.set(i, ((Number) row.getField(this.selectedColIndices[i])).doubleValue());
                }
                samples.add(v);
            }

            this.model.updateModel(samples, decayFactor);
            out.collect(this.model);
        }
    }

    public static class PredictOp extends RichCoFlatMapFunction<Row, Model, Row> {
        DirectReader.BatchStreamConnector modelConnector;
        private Model model = null;
        private int[] selectedColIndices = null;
        private DenseVector sample;
        private int[] keepColIdx = null;
        private String[] testColNames;

        public PredictOp(DirectReader.BatchStreamConnector modelConnector, String[] testColNames, int[] keepColIdx) {
            this.keepColIdx = keepColIdx;
            this.testColNames = testColNames;
            this.modelConnector = modelConnector;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.model = initModel(modelConnector);
            String[] featureColNames = model.featureColNames;
            selectedColIndices = new int[featureColNames.length];
            for (int i = 0; i < featureColNames.length; i++) {
                selectedColIndices[i] = TableUtil.findIndexFromName(testColNames, featureColNames[i]);
                if (selectedColIndices[i] < 0) {
                    throw new IllegalArgumentException(
                        "can't find feature col name in test data: " + featureColNames[i]);
                }
            }
            this.sample = new DenseVector(selectedColIndices.length);
        }

        @Override
        public void flatMap1(Row row, Collector<Row> out) throws Exception {
            for (int i = 0; i < this.selectedColIndices.length; i++) {
                sample.set(i, ((Number) row.getField(this.selectedColIndices[i])).doubleValue());
            }

            long clusterId = this.model.predict(sample);

            int keepSize = keepColIdx.length;
            Row r = new Row(keepSize + 1);
            for (int i = 0; i < keepSize; i++) {
                r.setField(i, row.getField(keepColIdx[i]));
            }

            r.setField(keepSize, clusterId);
            out.collect(r);
        }

        @Override
        public void flatMap2(Model value, Collector<Row> out) throws Exception {
            this.model = value;
        }
    }
}
