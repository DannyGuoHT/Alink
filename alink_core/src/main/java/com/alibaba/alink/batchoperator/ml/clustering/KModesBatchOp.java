package com.alibaba.alink.batchoperator.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ClusterConstant;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.string.DenseStringVector;
import com.alibaba.alink.common.ml.clustering.KModesModel;
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

import java.util.*;

/**
 * Partitioning a large set of objects into homogeneous clusters is a fundamental operation in data mining. The k-means
 * algorithm is best suited for implementing this operation because of its efficiency in clustering large data sets.
 * However, working only on numeric values limits its use in data mining because data sets in data mining often contain
 * categorical values. In this paper we present an algorithm, called k-modes, to extend the k-means paradigm to
 * categorical domains. We introduce new dissimilarity measures to deal with categorical objects, replace means of
 * clusters with modes, and use a frequency based method to update modes in the clustering process to minimise the
 * clustering cost function. Tested with the well known soybean disease data set the algorithm has demonstrated a very
 * good classification performance. Experiments on a very large health insurance data set consisting of half a million
 * records and 34 categorical attributes show that the algorithm is scalable in terms of both the number of clusters and
 * the number of records.
 * <p>
 * Huang, Zhexue. "A fast clustering algorithm to cluster very large categorical data sets in data mining." DMKD 3.8
 * (1997): 34-39.
 *
 * @<code>
 *     BatchOperator trainBatchOp = new KModesBatchOp( new AlinkParameter()
 *     .put(ParamName.k, 3)
 *     .put(ClusteringParamName.numIter,10)
 *     .put(ParamName.FeatureColNames,new String[]{"f0","f1","f2","f3","f4","f5"})
 * );
 * </code>
 *
 */
public class KModesBatchOp extends BatchOperator {

    /**
     * null constructor.
     */
    public KModesBatchOp() {
        super(null);
    }

    /**
     * this constructor has all parameter
     *
     * @param params
     */
    public KModesBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * set the the number of cluster
     *
     * @param k the number of cluster
     * @return this
     */
    public KModesBatchOp setK(int k) {
        this.putParamValue(ParamName.k, k);
        return this;
    }

    /**
     * set the feature column names to be process
     *
     * @param featureColNames
     * @return this
     */
    public KModesBatchOp setFeatureColNames(String[] featureColNames) {
        this.putParamValue(ParamName.featureColNames, featureColNames);
        return this;
    }

    /**
     * set the number of iteration
     *
     * @param numIter the number of iteration
     * @return this
     */
    public KModesBatchOp setNumIter(int numIter) {
        this.putParamValue(ParamName.numIter, numIter);
        return this;
    }

    /**
     * set the predict result column name of output table
     * @param predResultColName
     * @return this
     */
    public KModesBatchOp setPredResultColName(String predResultColName) {
        this.putParamValue(ParamName.predResultColName, predResultColName);
        return this;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        // get the input parameter's value
        final String[] featureColNames = params.getStringArrayOrDefault(ParamName.featureColNames,
            in.getSchema().getColumnNames());
        final int numIter = params.getIntegerOrDefault(ParamName.numIter, 10);
        final int k = params.getInteger(ParamName.k);
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
                .append(" as VARCHAR) as ")
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
            columnTypes.add(Types.STRING());
        }
        columnTypes.add(Types.LONG());
        columnTypes.add(Types.DOUBLE());

        final TableSchema outputSchema = new TableSchema(
            columnNames.toArray(new String[columnNames.size()]),
            columnTypes.toArray(new TypeInformation[columnTypes.size()])
        );

        try {
            // get the input data needed
            DataSet<DenseStringVector> data = in.select(sbd.toString()).getDataSet()
                .map(new MapFunction<Row, DenseStringVector>() {
                    @Override
                    public DenseStringVector map(Row row) throws Exception {
                        String[] values = new String[row.getArity()];
                        for (int i = 0; i < values.length; i++) {
                            values[i] = (String)row.getField(i);
                        }
                        return new DenseStringVector(values);
                    }
                });

            /**
             * initial the centroid
             * Tuple3: clusterId, clusterWeight, clusterCentroid
             */
            DataSet<Tuple3<Long, Double, DenseStringVector>> initCentroid = DataSetUtils
                .zipWithIndex(DataSetUtils.sampleWithSize(data, false, k))
                .map(new MapFunction<Tuple2<Long, DenseStringVector>, Tuple3<Long, Double, DenseStringVector>>() {
                    @Override
                    public Tuple3<Long, Double, DenseStringVector> map(Tuple2<Long, DenseStringVector> v)
                        throws Exception {
                        return new Tuple3<>(v.f0, 0., v.f1);
                    }
                })
                .withForwardedFields("f0->f0;f1->f2");

            IterativeDataSet<Tuple3<Long, Double, DenseStringVector>> loop = initCentroid.iterate(numIter);
            DataSet<Tuple2<Long, DenseStringVector>> samplesWithClusterId = assignClusterId(data, loop);
            DataSet<Tuple3<Long, Double, DenseStringVector>> updatedCentroid = updateCentroid(samplesWithClusterId,
                k, featureColNames.length);
            DataSet<Tuple3<Long, Double, DenseStringVector>> finalCentroid = loop.closeWith(updatedCentroid);

            // map the final centroid to row type, plus with the meta info
            DataSet<Row> modelRows = finalCentroid
                .mapPartition(new MapPartitionFunction<Tuple3<Long, Double, DenseStringVector>, Row>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Long, Double, DenseStringVector>> iterable,
                                             Collector<Row> out) throws Exception {
                        KModesModel model = new KModesModel(iterable);
                        model.getMeta().put(ParamName.featureColNames, featureColNames);
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
            DataSet<Tuple3<Long, Double, DenseStringVector>> clusterResult = calcClusterOfSample(data, finalCentroid);
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
    public static class MapToRow implements MapFunction<Tuple3<Long, Double, DenseStringVector>, Row> {
        @Override
        public Row map(Tuple3<Long, Double, DenseStringVector> value) throws Exception {
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
    private DataSet<Tuple2<Long, DenseStringVector>> assignClusterId(
        DataSet<DenseStringVector> data,
        DataSet<Tuple3<Long, Double, DenseStringVector>> centroids) {

        class FindClusterOp extends RichMapFunction<DenseStringVector, Tuple2<Long, DenseStringVector>> {
            List<Tuple3<Long, Double, DenseStringVector>> centroids;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                centroids = getRuntimeContext().getBroadcastVariable("centroids");
            }

            @Override
            public Tuple2<Long, DenseStringVector> map(DenseStringVector denseVector) throws Exception {
                long clusterId = KModesModel.findCluster(centroids, denseVector);
                return new Tuple2<>(clusterId, denseVector);
            }
        }

        return data.map(new FindClusterOp())
            .withBroadcastSet(centroids, "centroids");
    }


    /**
     * calculate the cluster result of sample
     *
     * @param data the whole sample data
     * @param centroids the centroids of clusters
     * @return  the DataSet of sample with clusterId and the distance to the center that the sample belongs
     */
    private DataSet<Tuple3<Long, Double, DenseStringVector>> calcClusterOfSample(
        DataSet<DenseStringVector> data,
        DataSet<Tuple3<Long, Double, DenseStringVector>> centroids) {

        class FindClusterOp extends RichMapFunction<DenseStringVector, Tuple3<Long, Double, DenseStringVector>> {
            List<Tuple3<Long, Double, DenseStringVector>> centroids;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                centroids = getRuntimeContext().getBroadcastVariable("centroids");
            }

            @Override
            public Tuple3<Long, Double, DenseStringVector> map(DenseStringVector denseVector) throws Exception {
                Tuple2<Long, Double> tuple2 = KModesModel.getCluster(centroids, denseVector);
                return new Tuple3<>(tuple2.f0, tuple2.f1, denseVector);
            }
        }

        return data.map(new FindClusterOp())
            .withBroadcastSet(centroids, "centroids");
    }

    /**
     * update centroid of cluster
     *
     * @param samplesWithClusterId sample with clusterId
     * @param k the number of clusters
     * @param dim the dim of featureColNames
     * @return the DataSet of center with clusterId and the weight(the number of samples belong to the cluster)
     */
    private DataSet<Tuple3<Long, Double, DenseStringVector>> updateCentroid(
        DataSet<Tuple2<Long, DenseStringVector>> samplesWithClusterId, final int k, final int dim) {

        // tuple3: clusterId, clusterWeight, clusterCentroid
        DataSet<Tuple3<Long, Double, Map<String, Integer>[]>> localAggregate =
            samplesWithClusterId.mapPartition(new DataPartition(k, dim));

        return localAggregate
            .groupBy(0)
            .reduce(new DataReduce(dim))
            .map(
                new MapFunction<Tuple3<Long, Double, Map<String, Integer>[]>, Tuple3<Long, Double,
                    DenseStringVector>>() {
                    @Override
                    public Tuple3<Long, Double, DenseStringVector> map(
                        Tuple3<Long, Double, Map<String, Integer>[]> in) {
                        return new Tuple3<>(in.f0, in.f1, new DenseStringVector(getKOfMaxV(in.f2)));
                    }
                })
            .withForwardedFields("f0;f1");
    }

    /**
     * calc local centroids
     */
    public static class DataPartition
        implements MapPartitionFunction<Tuple2<Long, DenseStringVector>, Tuple3<Long, Double, Map<String, Integer>[]>> {

        private int k;
        private int dim;

        public DataPartition(int k, int dim) {
            this.k = k;
            this.dim = dim;
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Long, DenseStringVector>> iterable,
                                 Collector<Tuple3<Long, Double, Map<String, Integer>[]>> collector) throws Exception {
            Map<String, Integer>[][] localCentroids = new HashMap[k][dim];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < dim; j++) {
                    localCentroids[i][j] = new HashMap<String, Integer>(32);
                }
            }
            double[] localCounts = new double[k];
            Arrays.fill(localCounts, 0.);

            for (Tuple2<Long, DenseStringVector> point : iterable) {
                int clusterId = point.f0.intValue();
                localCounts[clusterId] += 1.0;

                for (int j = 0; j < dim; j++) {
                    if (localCentroids[clusterId][j].containsKey(point.f1.get(j))) {
                        localCentroids[clusterId][j].put(point.f1.get(j),
                            localCentroids[clusterId][j].get(point.f1.get(j)) + 1);
                    } else {
                        localCentroids[clusterId][j].put(point.f1.get(j), 1);
                    }
                }
            }

            for (int i = 0; i < localCentroids.length; i++) {
                collector.collect(new Tuple3<>((long)i, localCounts[i], localCentroids[i]));
            }
        }
    }

    /**
     * calc Global Centroids
     */
    public static class DataReduce implements ReduceFunction<Tuple3<Long, Double, Map<String, Integer>[]>> {

        private int dim;

        public DataReduce(int dim) {
            this.dim = dim;
        }

        @Override
        public Tuple3<Long, Double, Map<String, Integer>[]> reduce(Tuple3<Long, Double, Map<String, Integer>[]> in1,
                                                                   Tuple3<Long, Double, Map<String, Integer>[]> in2) {
            return new Tuple3<>(in1.f0, in1.f1 + in2.f1, unionMaps(in1.f2, in2.f2, dim));
        }
    }

    /**
     * union two map,the same key will plus the value
     *
     * @param kv1 the 1th kv map
     * @param kv2 the 2th kv map
     * @return the map united
     */
    private static Map<String, Integer> unionMaps(Map<String, Integer> kv1, Map<String, Integer> kv2) {
        Map<String, Integer> kv = new HashMap<>();
        kv.putAll(kv1);
        for (String k : kv2.keySet()) {
            if (kv.containsKey(k)) {
                kv.put(k, kv.get(k) + kv2.get(k));
            } else {
                kv.put(k, kv2.get(k));
            }
        }
        return kv;
    }

    /**
     * union two map array
     *
     * @param kvArray1 the 1th kv array map
     * @param kvArray2 the 2th kv array map
     * @param dim the array's length
     * @return the map array united
     */
    private static Map<String, Integer>[] unionMaps(Map<String, Integer>[] kvArray1, Map<String, Integer>[] kvArray2,
                                                    int dim) {
        Map<String, Integer>[] kvArray = new HashMap[dim];
        for (int i = 0; i < dim; i++) {
            kvArray[i] = unionMaps(kvArray1[i], kvArray2[i]);
        }
        return kvArray;
    }

    /**
     * get the max key of map
     *
     * @param kv the kv map
     * @return the max key of map
     */
    private static String getKOfMaxV(Map<String, Integer> kv) {
        Integer tmp = Integer.MIN_VALUE;
        String k = null;
        for (Map.Entry<String, Integer> entry : kv.entrySet()) {
            if (entry.getValue() > tmp) {
                tmp = entry.getValue();
                k = entry.getKey();
            }
        }
        return k;
    }

    /**
     * get the max keys of map array
     *
     * @param kvs the kv map array
     * @return the max keys of map array
     */
    private static String[] getKOfMaxV(Map<String, Integer> kvs[]) {
        String[] ks = new String[kvs.length];
        for (int i = 0; i < kvs.length; i++) {
            ks[i] = getKOfMaxV(kvs[i]);
        }
        return ks;
    }

}
