package com.alibaba.alink.batchoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ClusterConstant;
import com.alibaba.alink.common.constants.ClusteringParamName;
import com.alibaba.alink.common.ml.clustering.DistanceType;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.ml.clustering.KMeansModel;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * k-means clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 * analysis in data mining. k-means clustering aims to partition n observations into k clusters in which each
 * observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 * <p>
 * (https://en.wikipedia.org/wiki/K-means_clustering) example:
 *

 * @<code>
 *     BatchOperator trainBatchOp = new KMeansBatchOp(new AlinkParameter()
 *     .put(ParamName.k, 4)
 *     .put(ClusteringParamName.NUM_ITER, 10)
 *     .put(ClusteringParamName.DISTANCE_TYPE, DistanceType.EUCLIDEAN)
 *     .put(ParamName.featureColNames,new String[]{"f0","f1","f2","f3","f4","f5"})
 *     };
 * </code>
 *
 */
public class KMeansBatchOp extends BatchOperator {

    /**
     * null constructor.
     */
    public KMeansBatchOp() {
        super(null);
    }

    /**
     * constructor.
     ** @param params the parameters set.
     */
    public KMeansBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * set the the number of cluster
     *
     * @param k the number of cluster
     * @return this
     */
    public KMeansBatchOp setK(int k) {
        this.putParamValue(ParamName.k, k);
        return this;
    }

    /**
     * set the number of iteration
     *
     * @param numIter the number of iteration
     * @return this
     */
    public KMeansBatchOp setNumIter(int numIter) {
        this.putParamValue(ParamName.numIter, numIter);
        return this;
    }

    /**
     * set the distanceType
     *
     * @param distanceType
     * @return this
     */
    public KMeansBatchOp setDistanceType(DistanceType distanceType) {
        this.putParamValue(ClusteringParamName.distanceType, distanceType);
        return this;
    }

    /**
     * set the feature column names to be process
     *
     * @param featureColNames
     * @return this
     */
    public KMeansBatchOp setFeatureColNames(String[] featureColNames) {
        this.putParamValue(ParamName.featureColNames, featureColNames);
        return this;
    }

    /**
     * set the predict result column name of output table
     * @param predResultColName
     * @return this
     */
    public KMeansBatchOp setPredResultColName(String predResultColName) {
        this.putParamValue(ParamName.predResultColName, predResultColName);
        return this;
    }

    /**
     * if only have one input table,the input is the sample table
     * @param in
     * @return this
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        List<BatchOperator> ls = new ArrayList();
        ls.add(in);
        return linkFrom(ls);
    }

    /**
     * have two input table. the 1th is the is the sample table; the 2th is initial center table,optional
     * @param ins
     * @return this
     */
    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {

        // get the input parameter's value
        final String[] featureColNames = params.getStringArrayOrDefault(ParamName.featureColNames,
            TableUtil.getNumericColNames(ins.get(0).getSchema()));
        final int numIter = params.getIntegerOrDefault(ParamName.numIter, 10);
        final int k = params.getInteger(ParamName.k);
        final DistanceType distanceType = params.getOrDefault(ClusteringParamName.distanceType, DistanceType.class,
            DistanceType.EUCLIDEAN);
        final String predResultColName = this.params.getStringOrDefault(ParamName.predResultColName,
            ClusterConstant.PRED_RESULT_COL_NAME);

        // construct the sql to get the input feature column name
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < featureColNames.length; i++) {
            if (i > 0) {
                sbd.append(", ");
            }
            sbd.append("cast(" )
                .append(featureColNames[i])
                .append(" as double) as ")
                .append(featureColNames[i]);
        }

        // the output outputSchema
        List<String> columnNames = new ArrayList<>();
        for (String col : featureColNames) {
            columnNames.add(col);
        }
        columnNames.add(predResultColName);
        columnNames.add(ClusterConstant.DISTANCE);

        List<TypeInformation> columnTypes = new ArrayList<>();
        for (String col : featureColNames) {
            columnTypes.add(Types.DOUBLE());
        }
        columnTypes.add(Types.LONG());
        columnTypes.add(Types.DOUBLE());

        final TableSchema outputSchema = new TableSchema(
            columnNames.toArray(new String[columnNames.size()]),
            columnTypes.toArray(new TypeInformation[columnTypes.size()])
        );

        try {
            // get the input data needed
            DataSet<DenseVector> data = ins.get(0).select(sbd.toString()).getDataSet()
                .map(new MapFunction<Row, DenseVector>() {
                    @Override
                    public DenseVector map(Row row) throws Exception {
                        double[] values = new double[row.getArity()];
                        for (int i = 0; i < values.length; i++) {
                            values[i] = (Double)row.getField(i);
                        }
                        return new DenseVector(values);
                    }
                });

            // Tuple3: clusterId, clusterWeight, clusterCentroid
            DataSet<Tuple3<Long, Double, DenseVector>> initCentroid = null;

            // initial the centroid
            if (ins.size() > 1 && null != ins.get(1)) {
                // if has the input center table
                StringBuilder sbd2 = new StringBuilder();
                sbd2.append(predResultColName).append(" AS ").append(predResultColName);
                for (int i = 0; i < featureColNames.length; i++) {
                    sbd2.append(", 1.0 * ").append(featureColNames[i]).append(" AS ").append(featureColNames[i]);
                }

                initCentroid = ins.get(1).select(sbd2.toString()).getDataSet()
                    .map(new MapFunction<Row, Tuple3<Long, Double, DenseVector>>() {
                        @Override
                        public Tuple3<Long, Double, DenseVector> map(Row row) throws Exception {
                            DenseVector denseVector = new DenseVector(row.getArity() - 1);
                            for (int i = 0; i < row.getArity() - 1; i++) {
                                denseVector.set(i, (Double)row.getField(i + 1));
                            }
                            return new Tuple3<>((Long)(row.getField(0)), 0., denseVector);
                        }
                    });
            } else {
                // if has no input center table,use random sample method
                initCentroid = DataSetUtils
                    .zipWithIndex(DataSetUtils.sampleWithSize(data, true, k))
                    .map(new MapFunction<Tuple2<Long, DenseVector>, Tuple3<Long, Double, DenseVector>>() {
                        @Override
                        public Tuple3<Long, Double, DenseVector> map(Tuple2<Long, DenseVector> v) throws Exception {
                            return new Tuple3<>(v.f0, 0., v.f1);
                        }
                    })
                    .withForwardedFields("f0->f0;f1->f2");
            }

            IterativeDataSet<Tuple3<Long, Double, DenseVector>> loop = initCentroid.iterate(numIter);
            DataSet<Tuple2<Long, DenseVector>> samplesWithClusterId = assignClusterId(data, loop, distanceType);
            DataSet<Tuple3<Long, Double, DenseVector>> updatedCentroid = updateCentroid(samplesWithClusterId, k,
                featureColNames.length, distanceType);
            DataSet<Tuple3<Long, Double, DenseVector>> finalCentroid = loop.closeWith(updatedCentroid);

            // map the final centroid to row type, plus with the meta info
            DataSet<Row> modelRows = finalCentroid
                .mapPartition(new MapPartitionFunction<Tuple3<Long, Double, DenseVector>, Row>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Long, Double, DenseVector>> iterable, Collector<Row> out)
                        throws Exception {
                        KMeansModel model = new KMeansModel(iterable);
                        model.getMeta().put(ParamName.featureColNames, featureColNames);
                        model.getMeta().put(ClusteringParamName.distanceType, distanceType);
                        model.getMeta().put(ParamName.k, k);

                        // meta plus data
                        List<Row> rows = model.save();
                        for (Row row : rows) {
                            out.collect(row);
                        }
                    }
                })
                .setParallelism(1);

            // store the clustering model to the sideTables[0]
            this.sideTables = new Table[] {
                RowTypeDataSet.toTable(modelRows, AlinkModel.DEFAULT_MODEL_SCHEMA)
            };

            // Tuple3: clusterId, distance, sample
            DataSet<Tuple3<Long, Double, DenseVector>> clusterResult = calcClusterOfSample(data, finalCentroid,
                distanceType);
            DataSet<Row> rowDataSet = clusterResult.map(new MapToRow());

            // store the clustering result the table
            this.table = RowTypeDataSet.toTable(rowDataSet, outputSchema);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }

        return this;
    }

    /**
     * map clustering result to Row format
     */
    public static class MapToRow implements MapFunction<Tuple3<Long, Double, DenseVector>, Row> {
        @Override
        public Row map(Tuple3<Long, Double, DenseVector> value) throws Exception {
            Row row = new Row(value.f2.size() + 2);
            for (int i = 0; i < value.f2.size(); i++) {
                row.setField(i, value.f2.get(i));
            }
            row.setField(value.f2.size(), value.f0);
            row.setField(value.f2.size() + 1, value.f1);
            return row;
        }
    }

    /**
     * assign clusterId to sample
     *
     * @param data the whole sample data
     * @param centroids the centroids of clusters
     * @return the DataSet of sample with clusterId
     */
    private DataSet<Tuple2<Long, DenseVector>> assignClusterId(
        DataSet<DenseVector> data,
        DataSet<Tuple3<Long, Double, DenseVector>> centroids,
        DistanceType distanceType) {

        class FindClusterOp extends RichMapFunction<DenseVector, Tuple2<Long, DenseVector>> {
            private DistanceType distanceType;
            List<Tuple3<Long, Double, DenseVector>> centroids;

            public FindClusterOp(DistanceType distanceType) {
                this.distanceType = distanceType;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                centroids = getRuntimeContext().getBroadcastVariable("centroids");
            }

            @Override
            public Tuple2<Long, DenseVector> map(DenseVector denseVector) throws Exception {
                long clusterId = KMeansModel.findCluster(centroids, denseVector, distanceType);
                return new Tuple2<>(clusterId, denseVector);
            }
        }

        return data.map(new FindClusterOp(distanceType))
            .withBroadcastSet(centroids, "centroids");
    }

    /**
     * calculate the cluster result of sample
     *
     * @param data the whole sample data
     * @param centroids the centroids of clusters
     * @param distanceType the distance type to calculate distance
     * @return  the DataSet of sample with clusterId and the distance to the center that the sample belongs
     */
    private DataSet<Tuple3<Long, Double, DenseVector>> calcClusterOfSample(
        DataSet<DenseVector> data,
        DataSet<Tuple3<Long, Double, DenseVector>> centroids,
        DistanceType distanceType) {

        class FindClusterOp extends RichMapFunction<DenseVector, Tuple3<Long, Double, DenseVector>> {
            private DistanceType distanceType;
            List<Tuple3<Long, Double, DenseVector>> centroids;

            public FindClusterOp(DistanceType distanceType) {
                this.distanceType = distanceType;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                centroids = getRuntimeContext().getBroadcastVariable("centroids");
            }

            @Override
            public Tuple3<Long, Double, DenseVector> map(DenseVector denseVector) throws Exception {
                Tuple2<Long, Double> tuple2 = KMeansModel.getCluster(centroids, denseVector, distanceType);
                return new Tuple3<>(tuple2.f0, tuple2.f1, denseVector);
            }
        }

        return data.map(new FindClusterOp(distanceType))
            .withBroadcastSet(centroids, "centroids");
    }

    /**
     * update centroid of cluster
     *
     * @param samplesWithClusterId sample with clusterId
     * @param k the number of clusters
     * @param dim the dim of featureColNames
     * @param distanceType the distance type to calculate distance
     * @return the DataSet of center with clusterId and the weight(the number of samples belong to the cluster)
     */
    private DataSet<Tuple3<Long, Double, DenseVector>> updateCentroid(
        DataSet<Tuple2<Long, DenseVector>> samplesWithClusterId, final int k, final int dim,
        final DistanceType distanceType) {

        // tuple3: clusterId, clusterWeight, clusterCentroid
        DataSet<Tuple3<Long, Double, DenseVector>> localAggregate =
            samplesWithClusterId.mapPartition(
                new MapPartitionFunction<Tuple2<Long, DenseVector>, Tuple3<Long, Double, DenseVector>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, DenseVector>> iterable,
                                             Collector<Tuple3<Long, Double, DenseVector>> collector) throws Exception {
                        DenseVector[] localCentroids = new DenseVector[k];
                        double[] localCounts = new double[k];
                        Arrays.fill(localCounts, 0.);
                        for (int i = 0; i < localCentroids.length; i++) {
                            localCentroids[i] = new DenseVector(dim);
                        }

                        for (Tuple2<Long, DenseVector> point : iterable) {
                            int which = point.f0.intValue();
                            localCounts[which] += 1.0;
                            localCentroids[which].plusEqual(point.f1);
                        }

                        for (int i = 0; i < localCentroids.length; i++) {
                            collector.collect(new Tuple3<>((long)i, localCounts[i], localCentroids[i]));
                        }
                    }
                });

        return localAggregate
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple3<Long, Double, DenseVector>>() {
                @Override
                public Tuple3<Long, Double, DenseVector> reduce(Tuple3<Long, Double, DenseVector> in1,
                                                                Tuple3<Long, Double, DenseVector> in2)
                    throws Exception {
                    return new Tuple3<>(in1.f0, in1.f1 + in2.f1, in1.f2.plus(in2.f2));
                }
            })
            .map(new MapFunction<Tuple3<Long, Double, DenseVector>, Tuple3<Long, Double, DenseVector>>() {
                @Override
                public Tuple3<Long, Double, DenseVector> map(Tuple3<Long, Double, DenseVector> in) throws Exception {
                    if (in.f1 < 0.5) {
                        return in;
                    } else {
                        DenseVector vector = in.f2.scale(1.0 / in.f1);
                        if (DistanceType.HAMMING.equals(distanceType)) {
                            vector.roundEqual();
                        }
                        return new Tuple3<>(in.f0, in.f1, vector);
                    }
                }
            })
            .withForwardedFields("f0;f1");
    }

}
