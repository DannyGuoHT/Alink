package com.alibaba.alink.common.recommendation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.alibaba.alink.common.AlinkParameter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import static java.lang.Math.abs;

/**
 * The ALS kernel.
 */
public class ALS {
    public ALSModel model;
    final private int numBlocksPerTask = 1;

    final private int numFactors;
    final private int numIter;
    final private int topK;
    final private double lambda;

    final private boolean implicitPref;
    final private double alpha;

    public static class ALSModel {
        public DataSet <Tuple2 <Integer, float[]>> userFactors;
        public DataSet <float[]> userAvgFactors;
        public DataSet <Tuple2 <Integer, float[]>> itemFactors;
        public DataSet <float[]> itemAvgFactors;
        public DataSet <Tuple3 <Integer, Integer, Long>> graphCounts;

        public DataSet <Tuple3 <Byte, Long, float[]>> getFactors() {
            DataSet <Tuple3 <Byte, Long, float[]>> outputUserFactors = userFactors
                .map(new MapFunction <Tuple2 <Integer, float[]>, Tuple3 <Byte, Long, float[]>>() {
                    @Override
                    public Tuple3 <Byte, Long, float[]> map(Tuple2 <Integer, float[]> value) throws Exception {
                        return Tuple3.of((byte)0, (long)value.f0, value.f1);
                    }
                });

            DataSet <Tuple3 <Byte, Long, float[]>> outputItemFactors = itemFactors
                .map(new MapFunction <Tuple2 <Integer, float[]>, Tuple3 <Byte, Long, float[]>>() {
                    @Override
                    public Tuple3 <Byte, Long, float[]> map(Tuple2 <Integer, float[]> value) throws Exception {
                        return Tuple3.of((byte)1, (long)value.f0, value.f1);
                    }
                });

            return outputUserFactors.union(outputItemFactors);
        }
    }

    private static class BlockPartitioner {
        public static int[] getBlockStarts(int numBlocks, int numElements) {
            int blockSize = numElements / numBlocks;
            int remain = numElements % numBlocks;
            int[] starts = new int[numBlocks];
            for (int i = 0; i < numBlocks; i++) {
                starts[i] = i * blockSize + Math.min(i, remain);
            }
            return starts;
        }

        public static int[] getBlockCounts(int numBlocks, int numElements) {
            int blockSize = numElements / numBlocks;
            int remain = numElements % numBlocks;
            int[] count = new int[numBlocks];
            for (int i = 0; i < numBlocks; i++) {
                count[i] = blockSize + (i < remain ? 1 : 0);
            }
            return count;
        }

        public static int getBlockId(int[] blockStarts, int elementId) {
            int pos = Arrays.binarySearch(blockStarts, elementId + 1);
            if (pos < 0) {
                pos = -(pos + 1);
            } else if (pos > 0) {
                // to handle a special case that numElements > numBlocks
                while (pos > 0 && blockStarts[pos] == blockStarts[pos - 1]) { pos--; }
            } else {
                throw new RuntimeException("unexpected");
            }
            return pos - 1;
        }
    }

    public ALS(AlinkParameter params) {

        this.numFactors = params.getIntegerOrDefault("numFactors", 10);
        this.numIter = params.getIntegerOrDefault("numIter", 10);
        this.topK = params.getIntegerOrDefault("topK", 100);
        this.lambda = params.getDoubleOrDefault("lambda", 0.1);

        this.implicitPref = params.getBoolOrDefault("implicitPref", false);
        this.alpha = params.getDoubleOrDefault("alpha", 40.0);
    }

    public void fit(DataSet <Tuple3 <Integer, Integer, Float>> ratings) throws Exception {
        final int numTasks = ratings.getExecutionEnvironment().getParallelism();
        final int numBlocks = numTasks * numBlocksPerTask;

        DataSet <Tuple3 <Integer, Integer, Long>> graphCounts = getGraphCounts(ratings);

        // tuple4: blockId, nodeId, neighbors, ratings
        DataSet <Tuple4 <Integer, Integer, int[], float[]>> userGraph = constructGraph(ratings, graphCounts, numBlocks,
            0);
        DataSet <Tuple4 <Integer, Integer, int[], float[]>> itemGraph = constructGraph(ratings, graphCounts, numBlocks,
            1);

        // tuple5: blockId, nodeId, neighbors, ratings, factors
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> userStat = userGraph
            .map(new InitFactorOp(numFactors))
            .withForwardedFields("f0;f1;f2;f3");
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> itemStat = itemGraph
            .map(new InitFactorOp(numFactors))
            .withForwardedFields("f0;f1;f2;f3");

        // Iterate
        // tuple5: blockId, nodeId, neighbors, ratings, factors
        IterativeDataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> itemLoop = itemStat.iterate(numIter);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> tempUserStat =
            computeFactors(itemLoop, userStat, graphCounts, numBlocks, numFactors, 0, 1);
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> tempItemStat =
            computeFactors(tempUserStat, itemStat, graphCounts, numBlocks, numFactors, 1, 0);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> finalItemStat = itemLoop.closeWith(tempItemStat);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> finalUserStat =
            computeFactors(finalItemStat, userStat, graphCounts, numBlocks, numFactors, 0, 1);

        model = new ALSModel();

        model.itemFactors = finalItemStat.project(1, 4);
        model.userFactors = finalUserStat.project(1, 4);

        model.itemAvgFactors = reduceFactors(model.itemFactors, numFactors);
        model.userAvgFactors = reduceFactors(model.userFactors, numFactors);
        model.graphCounts = graphCounts;
    }

    // tuple3: userId, topKItemId, topKRating
    public DataSet <Tuple3 <Integer, List <Integer>, List <Float>>>
    recommend(DataSet <Tuple3 <Integer, Integer, Float>> ratings, boolean recForUnknown) throws Exception {

        final int numTasks = ratings.getExecutionEnvironment().getParallelism();
        final int numBlocks = numTasks * numBlocksPerTask;

        DataSet <Tuple3 <Integer, Integer, Long>> graphCounts = getGraphCounts(ratings);

        // tuple4: blockId, nodeId, neighbors, ratings
        DataSet <Tuple4 <Integer, Integer, int[], float[]>> userGraph = constructGraph(ratings, graphCounts, numBlocks,
            0);
        DataSet <Tuple4 <Integer, Integer, int[], float[]>> itemGraph = constructGraph(ratings, graphCounts, numBlocks,
            1);

        // tuple5: blockId, nodeId, neighbors, ratings, factors
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> userStat = userGraph
            .map(new InitFactorOp(numFactors))
            .withForwardedFields("f0;f1;f2;f3");
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> itemStat = itemGraph
            .map(new InitFactorOp(numFactors))
            .withForwardedFields("f0;f1;f2;f3");

        // Iterate
        // tuple5: blockId, nodeId, neighbors, ratings, factors
        IterativeDataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> itemLoop = itemStat.iterate(numIter);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> tempUserStat =
            computeFactors(itemLoop, userStat, graphCounts, numBlocks, numFactors, 0, 1);
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> tempItemStat =
            computeFactors(tempUserStat, itemStat, graphCounts, numBlocks, numFactors, 1, 0);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> finalItemStat = itemLoop.closeWith(tempItemStat);

        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> finalUserStat =
            computeFactors(finalItemStat, userStat, graphCounts, numBlocks, numFactors, 0, 1);

        // Compute topK recommendations for users and items

        if (numTasks != numBlocks) { throw new RuntimeException("unexpected"); }

        // tuple3: taskId, userId, userFactor
        DataSet <Tuple3 <Integer, Integer, float[]>> userFactor = finalUserStat.project(0, 1, 4);
        // tuple3: taskId, itemId, itemFactor
        DataSet <Tuple3 <Integer, Integer, float[]>> itemFactor = finalItemStat.project(0, 1, 4);
        // tuple2: userId, userNeighbor
        DataSet <Tuple2 <Integer, int[]>> userNeighbor = finalUserStat.project(1, 2);

        // tuple5: taskId, userId, userNeighbors, userFactors, <topkItem, topkRate>
        DataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> userTopk = initTopk(userFactor,
            userNeighbor);

        // Iterate block by block to compute top k recommendation
        // tuple5: taskId, userId, userNeighbors, userFactors, <topkItem, topkRate>
        IterativeDataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> topkLoop = userTopk
            .iterate(numTasks);

        // dummy dataset just to ensure that following 'itemFactor' are connected with iterative environment
        DataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> dummy = topkLoop.first(1);

        // tuple4: taskId, itemId, itemFactors, targetTaskId
        DataSet <Tuple4 <Integer, Integer, float[], Integer>> shiftedItemFactor = itemFactor
            .map(
                new RichMapFunction <Tuple3 <Integer, Integer, float[]>, Tuple4 <Integer, Integer, float[], Integer>>
                    () {
                    @Override
                    public Tuple4 <Integer, Integer, float[], Integer> map(Tuple3 <Integer, Integer, float[]> value)
                        throws Exception {
                        int stepNo = getIterationRuntimeContext().getSuperstepNumber();
                        int userBlockId = (value.f0 + stepNo) % numTasks;
                        return new Tuple4 <>(value.f0, value.f1, value.f2, userBlockId);
                    }
                })
            .withForwardedFields("f0;f1;f2")
            .withBroadcastSet(dummy, "dummy");

        // tuple5: taskId, userId, userNeighbors, userFactors, <topkItem, topkRate>
        DataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> updatedUserTopk = topkLoop
            .coGroup(shiftedItemFactor)
            .where(0).equalTo(3)
            .withPartitioner(new CustomBlockPartitioner())
            .with(new UpdateUserTopkOp(topK))
            .withForwardedFieldsFirst("f0;f1;f2;f3");

        DataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> finalUserTopk = topkLoop
            .closeWith(updatedUserTopk);

        // generate recommendation result
        DataSet <Tuple3 <Integer, List <Integer>, List <Float>>> rec = finalUserTopk
            . <Tuple2 <Integer, TreeMap <Float, Integer>>>project(1, 4)
            .map(
                new MapFunction <Tuple2 <Integer, TreeMap <Float, Integer>>, Tuple3 <Integer, List <Integer>, List
                    <Float>>>() {
                    @Override
                    public Tuple3 <Integer, List <Integer>, List <Float>> map(
                        Tuple2 <Integer, TreeMap <Float, Integer>> value) throws Exception {
                        TreeMap <Float, Integer> topk = value.f1;

                        List <Integer> topKNeighbors = new ArrayList <>(topk.size());
                        List <Float> topKScores = new ArrayList <>(topk.size());
                        Set set = topk.entrySet();
                        Iterator it = set.iterator();
                        while (it.hasNext()) {
                            Map.Entry me = (Map.Entry)it.next();
                            topKNeighbors.add((Integer)me.getValue());
                            topKScores.add(-(Float)me.getKey());
                        }

                        return new Tuple3 <>(value.f0, topKNeighbors, topKScores);
                    }
                })
            .withForwardedFields("f0");

        return rec;
    }

    // tuple5: taskId, userId, userNeighbors, userFactors, <topkItem, topkRate>
    private DataSet <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>>
    initTopk(DataSet <Tuple3 <Integer, Integer, float[]>> userFactor, DataSet <Tuple2 <Integer, int[]>> userNeighbor) {

        DataSet <Tuple4 <Integer, Integer, int[], float[]>> user = userFactor
            .join(userNeighbor).where(1).equalTo(0)
            .projectFirst(0, 1)
            .projectSecond(1)
            .projectFirst(2);

        return user
            .map(
                new MapFunction <Tuple4 <Integer, Integer, int[], float[]>, Tuple5 <Integer, Integer, int[], float[],
                    TreeMap <Float, Integer>>>() {
                    @Override
                    public Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>> map(
                        Tuple4 <Integer, Integer, int[], float[]> value) throws Exception {
                        return new Tuple5 <>(value.f0, value.f1, value.f2, value.f3, new TreeMap <Float, Integer>());
                    }
                })
            .withForwardedFields("f0;f1;f2;f3")
            .partitionCustom(new CustomBlockPartitioner(), 0);
    }

    private static class UpdateUserTopkOp extends RichCoGroupFunction <
        Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>,
        Tuple4 <Integer, Integer, float[], Integer>,
        Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> {

        private int topK;

        public UpdateUserTopkOp(int topK) {
            this.topK = topK;
        }

        @Override
        public void coGroup(Iterable <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> first,
                            Iterable <Tuple4 <Integer, Integer, float[], Integer>> second,
                            Collector <Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>>> out)
            throws Exception {

            if (null == first) { return; }

            try {
                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                int stepNo = getIterationRuntimeContext().getSuperstepNumber();
                System.out.println("[" + taskId + "]: " + "TopK step " + stepNo);
            } catch (Exception e) {
                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("[" + taskId + "]: " + "TopK step");
            }

            List <Tuple4 <Integer, Integer, float[], Integer>> buffer = new ArrayList <>();

            if (null != second) {
                for (Tuple4 <Integer, Integer, float[], Integer> item : second) { buffer.add(item); }
            }

            for (Tuple5 <Integer, Integer, int[], float[], TreeMap <Float, Integer>> user : first) {
                if (buffer.size() == 0) {
                    out.collect(user);
                    continue;
                }

                TreeMap <Float, Integer> topk = user.f4;
                final float[] userFactor = user.f3;
                final int[] userNeighbors = user.f2;

                float head = Float.POSITIVE_INFINITY;
                if (topk.size() > 0) { head = topk.lastKey(); }

                for (Tuple4 <Integer, Integer, float[], Integer> item : buffer) {
                    final float[] itemFactor = item.f2;

                    if (Arrays.binarySearch(userNeighbors, item.f1) >= 0) { continue; }

                    float v = 0.F;
                    for (int k = 0; k < userFactor.length; k++) { v += userFactor[k] * itemFactor[k]; }
                    v = -v;

                    if (topk.size() < topK) {
                        topk.put(v, item.f1);
                        head = topk.lastKey();
                    } else {
                        if (v < head) {
                            topk.put(v, item.f1);
                            topk.pollLastEntry();
                            head = topk.lastKey();
                        }
                    }
                }

                out.collect(user);
            }

        }
    }

    // tuple3: numUsers, numItems, numEdges
    private DataSet <Tuple3 <Integer, Integer, Long>> getGraphCounts(
        DataSet <Tuple3 <Integer, Integer, Float>> ratings) {
        return ratings
            . <Tuple3 <Integer, Integer, Long>>mapPartition(
                new MapPartitionFunction <Tuple3 <Integer, Integer, Float>, Tuple3 <Integer, Integer, Long>>() {
                    @Override
                    public void mapPartition(Iterable <Tuple3 <Integer, Integer, Float>> edges,
                                             Collector <Tuple3 <Integer, Integer, Long>> out) throws Exception {
                        int maxUserId = 0;
                        int maxItemId = 0;
                        long cnt = 0L;
                        for (Tuple3 <Integer, Integer, Float> edge : edges) {
                            maxUserId = Math.max(edge.f0, maxUserId);
                            maxItemId = Math.max(edge.f1, maxItemId);
                            cnt++;
                        }
                        out.collect(new Tuple3 <>(maxUserId, maxItemId, cnt));
                    }
                })
            .reduce(new ReduceFunction <Tuple3 <Integer, Integer, Long>>() {
                @Override
                public Tuple3 <Integer, Integer, Long> reduce(Tuple3 <Integer, Integer, Long> value1,
                                                              Tuple3 <Integer, Integer, Long> value2) throws Exception {
                    return new Tuple3 <>(Math.max(value1.f0, value2.f0), Math.max(value1.f1, value2.f1),
                        value1.f2 + value2.f2);
                }
            })
            .map(new MapFunction <Tuple3 <Integer, Integer, Long>, Tuple3 <Integer, Integer, Long>>() {
                @Override
                public Tuple3 <Integer, Integer, Long> map(Tuple3 <Integer, Integer, Long> value) throws Exception {
                    System.out.println("num user = " + (value.f0 + 1));
                    System.out.println("num item = " + (value.f1 + 1));
                    System.out.println("num edge = " + (value.f2));
                    return new Tuple3 <>(value.f0 + 1, value.f1 + 1, value.f2);
                }
            });
    }

    private static class CustomBlockPartitioner implements Partitioner <Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    private DataSet <Tuple4 <Integer, Integer, int[], float[]>>
    constructGraph(DataSet <Tuple3 <Integer, Integer, Float>> ratings,
                   DataSet <Tuple3 <Integer, Integer, Long>> graphCounts,
                   int numBlocks,
                   int who) {
        final int src = (who == 0 ? 0 : 1);
        final int dst = (who == 0 ? 1 : 0);

        return ratings
            .groupBy(src)
            .sortGroup(dst, Order.ASCENDING)
            .reduceGroup(new ConstructGraphRowOp(src, dst, numBlocks))
            .withBroadcastSet(graphCounts, "graphCounts")
            .partitionCustom(new CustomBlockPartitioner(), 0);
    }

    private static class ConstructGraphRowOp extends RichGroupReduceFunction <
        Tuple3 <Integer, Integer, Float>, Tuple4 <Integer, Integer, int[], float[]>> {
        private int src;
        private int dst;
        private List <Tuple3 <Integer, Integer, Long>> graphCounts = null;
        private int numBlocks;
        private int[] srcStarts = null;
        private int[] srcCounts = null;

        public ConstructGraphRowOp(int src, int dst, int numBlocks) {
            this.src = src;
            this.dst = dst;
            this.numBlocks = numBlocks;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.graphCounts = getRuntimeContext().getBroadcastVariable("graphCounts");
            int numSrcs = graphCounts.get(0).getField(src);
            this.srcStarts = BlockPartitioner.getBlockStarts(numBlocks, numSrcs);
            this.srcCounts = BlockPartitioner.getBlockCounts(numBlocks, numSrcs);
        }

        @Override
        public void reduce(Iterable <Tuple3 <Integer, Integer, Float>> values,
                           Collector <Tuple4 <Integer, Integer, int[], float[]>> out) throws Exception {
            List <Tuple3 <Integer, Integer, Float>> buffer = new ArrayList <>();
            for (Tuple3 <Integer, Integer, Float> v : values) { buffer.add(v); }

            int[] neighbors = new int[buffer.size()];
            float[] ratings = new float[buffer.size()];

            int pos = 0;
            int srcNodeId = -1;
            for (Tuple3 <Integer, Integer, Float> v : buffer) {
                srcNodeId = v.getField(src);
                neighbors[pos] = v.getField(dst);
                ratings[pos] = v.getField(2);
                pos++;
            }

            int srcBlockId = BlockPartitioner.getBlockId(srcStarts, srcNodeId);
            out.collect(new Tuple4 <>(srcBlockId, srcNodeId, neighbors, ratings));
        }
    }

    public static class NormalEquation {
        public int k; // number of factors
        public double[][] ata;
        public double[] atb;

        private double[] da;

        public NormalEquation(int k) {
            this.k = k;
            this.ata = new double[k][k];
            this.atb = new double[k];
            this.da = new double[k];
        }

        private void copyToDouble(float[] a) {
            for (int i = 0; i < a.length; i++) {
                this.da[i] = (double)a[i];
            }
        }

        /**
         * @param a: factors of a user/item
         * @param b: rating
         * @param c: confidence - 1 for implicit case; 1.0 for explicit case
         */
        public void add(float[] a, double b, double c) {
            copyToDouble(a);

            // blas.dspr
            // A: A + alpha*x*x'
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    ata[i][j] += da[i] * da[j] * c;
                }
            }

            if (b != 0.0) {
                for (int i = 0; i < k; i++) {
                    atb[i] += da[i] * b;
                }
            }
        }

        public void reset() {
            for (int i = 0; i < k; i++) {
                Arrays.fill(ata[i], 0.);
            }
            Arrays.fill(atb, 0.);
        }

        public void merge(double[][] otherAta) {
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    ata[i][j] += otherAta[i][j];
                }
            }
        }
    }

    public static class LeastSquaresNESolver {
        public static float[] solve(NormalEquation ne, double regulerizeParam) {
            int k = ne.k;
            for (int i = 0; i < k; i++) {
                ne.ata[i][i] += regulerizeParam;
            }

            int err = DenseSolver.solve(ne.ata, ne.atb, ne.k);
            if (err != 0) {
                throw new RuntimeException("Fail to solve linear equations.");
            }

            float[] x = new float[k];
            for (int i = 0; i < x.length; i++) {
                x[i] = (float)ne.atb[i];
            }

            return x;
        }
    }

    public static class DenseSolver {
        public static int solve(double[][] a, double[] b, int n) {
            int i, j, k;
            double d, r;

            for (i = 0; i < n; i++) {
                d = abs(a[i][i]);
                k = i;
                for (j = i + 1; j < n; j++) {
                    if ((r = abs(a[j][i])) > d) {
                        d = r;
                        k = j;
                    }
                }
                if (abs(d) < 1.0e-16) {
                    return 1; /* fail to solve */
                }
                if (k != i) {
                    /* exchange row i with row k */
                    for (j = i; j < n; j++) {
                        d = a[i][j];
                        a[i][j] = a[k][j];
                        a[k][j] = d;
                    }
                    d = b[i];
                    b[i] = b[k];
                    b[k] = d;
                }
                if ((d = a[i][i]) != 1.0) {
                    d = 1.0 / d;
                    for (j = i + 1; j < n; j++) { a[i][j] *= d; }
                    b[i] *= d;
                }
                for (j = i + 1; j < n; j++) {
                    if ((d = a[j][i]) == 0.0) { continue; }
                    for (k = i + 1; k < n; k++) { a[j][k] -= d * a[i][k]; }
                    b[j] -= d * b[i];
                }
            }

            for (i = n - 2; i >= 0; i--) {
                for (j = i + 1; j < n; j++) {
                    d = a[i][j];
                    b[i] -= d * b[j];
                }
            }

            return 0;
        }
    }

    private DataSet <double[][]>
    computeYtY(DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> factors, final int numFactors) {
        DataSet <double[][]> YtY = factors
            .mapPartition(new MapPartitionFunction <Tuple5 <Integer, Integer, int[], float[], float[]>, double[][]>() {
                @Override
                public void mapPartition(Iterable <Tuple5 <Integer, Integer, int[], float[], float[]>> values,
                                         Collector <double[][]> out) throws Exception {
                    double[][] blockYtY = new double[numFactors][numFactors];
                    for (int i = 0; i < numFactors; i++) {
                        Arrays.fill(blockYtY[i], 0.);
                    }
                    for (Tuple5 <Integer, Integer, int[], float[], float[]> v : values) {
                        float[] factors = v.f4;
                        for (int i = 0; i < numFactors; i++) {
                            for (int j = 0; j < numFactors; j++) {
                                blockYtY[i][j] += factors[i] * factors[j];
                            }
                        }
                    }
                    out.collect(blockYtY);
                }
            })
            .reduce(new ReduceFunction <double[][]>() {
                @Override
                public double[][] reduce(double[][] value1, double[][] value2) throws Exception {
                    double[][] sum = new double[numFactors][numFactors];
                    for (int i = 0; i < numFactors; i++) {
                        for (int j = 0; j < numFactors; j++) {
                            sum[i][j] = value1[i][j] + value2[i][j];
                        }
                    }
                    return sum;
                }
            });
        return YtY;
    }

    // send factors from src to dst
    private static class SendFactorsOp extends RichFlatMapFunction <
        Tuple5 <Integer, Integer, int[], float[], float[]>,
        Tuple4 <Integer, Integer, Integer, float[]>> {

        private int src;
        private int dst;
        private List <Tuple3 <Integer, Integer, Long>> graphCounts = null;
        private int numBlocks;
        private int numFactors;
        private int[] dstStarts = null;
        private int[] dstCounts = null;

        public SendFactorsOp(int src, int dst, int numBlocks, int numFactors) {
            this.src = src;
            this.dst = dst;
            this.numBlocks = numBlocks;
            this.numFactors = numFactors;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.graphCounts = getRuntimeContext().getBroadcastVariable("graphCounts");
            int numDsts = graphCounts.get(0).getField(dst);
            this.dstStarts = BlockPartitioner.getBlockStarts(numBlocks, numDsts);
            this.dstCounts = BlockPartitioner.getBlockCounts(numBlocks, numDsts);
        }

        @Override
        public void flatMap(Tuple5 <Integer, Integer, int[], float[], float[]> srcNodeStat,
                            Collector <Tuple4 <Integer, Integer, Integer, float[]>> out) throws Exception {

            final Integer srcBlockId = srcNodeStat.f0;
            final Integer srcNodeId = srcNodeStat.f1;
            final int[] srcNeighbors = srcNodeStat.f2;
            final float[] srcNbRating = srcNodeStat.f3;
            final float[] srcFactors = srcNodeStat.f4;

            byte[] flag = new byte[numBlocks];
            Arrays.fill(flag, (byte)0);

            for (int i = 0; i < srcNeighbors.length; i++) {
                int dstNodeId = srcNeighbors[i];
                int dstBlockId = BlockPartitioner.getBlockId(dstStarts, dstNodeId);
                flag[dstBlockId] = 1;
            }

            for (int i = 0; i < numBlocks; i++) {
                if (flag[i] < 1) { continue; }

                out.collect(new Tuple4 <>(i, srcBlockId, srcNodeId, srcFactors));
            }
        }
    }

    // compute factors of 'src', using factors of 'dst'
    private DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>>
    computeFactors(DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> dstStat,
                   DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> srcStat,
                   DataSet <Tuple3 <Integer, Integer, Long>> graphCounts,
                   int numBlocks, int numFactors, int src, int dst) {

        // send dst factors blocks to src block
        // Tuple4: srcBlockId, dstBlockId, dstNodeId, factorsToSendFromDstBlockToSrcBlock
        DataSet <Tuple4 <Integer, Integer, Integer, float[]>> sendDstFactors = dstStat
            .flatMap(new SendFactorsOp(dst, src, numBlocks, numFactors))
            .withBroadcastSet(graphCounts, "graphCounts");

        // Tuple2: dstBlockId, dstFactors
        DataSet <Tuple5 <Integer, Integer, int[], float[], float[]>> updatedSrcStat = null;

        if (implicitPref) {
            DataSet <double[][]> YtY = computeYtY(dstStat, numFactors);

            updatedSrcStat = srcStat
                .coGroup(sendDstFactors).where(0).equalTo(0)
                .sortSecondGroup(2, Order.ASCENDING)
                .withPartitioner(new CustomBlockPartitioner())
                .with(new UpdateFactorImplicitOp(numFactors, lambda, alpha, src, dst))
                .withBroadcastSet(YtY, "YtY")
                .withForwardedFieldsFirst("f0;f1;f2;f3");
        } else {
            updatedSrcStat = srcStat
                .coGroup(sendDstFactors).where(0).equalTo(0)
                .sortSecondGroup(2, Order.ASCENDING)
                .withPartitioner(new CustomBlockPartitioner())
                .with(new UpdateFactorExplicitOp(numFactors, lambda, src, dst))
                .withForwardedFieldsFirst("f0;f1;f2;f3");
        }

        return updatedSrcStat;
    }

    public static class InitFactorOp extends RichMapFunction <
        Tuple4 <Integer, Integer, int[], float[]>,
        Tuple5 <Integer, Integer, int[], float[], float[]>> {
        private Random random = null;
        private int numFactors;

        public InitFactorOp(int numFactors) {
            this.numFactors = numFactors;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.random = new Random((long)getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Tuple5 <Integer, Integer, int[], float[], float[]> map(Tuple4 <Integer, Integer, int[], float[]> value)
            throws Exception {
            float[] factors = new float[numFactors];
            for (int j = 0; j < numFactors; j++) {
                factors[j] = random.nextFloat();
            }
            return new Tuple5 <>(value.f0, value.f1, value.f2, value.f3, factors);
        }
    }

    private DataSet <float[]>
    reduceFactors(DataSet <Tuple2 <Integer, float[]>> factors, final int numFactors) {
        DataSet <float[]> reducedFactors = factors
            .mapPartition(new MapPartitionFunction <Tuple2 <Integer, float[]>, Tuple2 <Long, float[]>>() {
                @Override
                public void mapPartition(Iterable <Tuple2 <Integer, float[]>> values,
                                         Collector <Tuple2 <Long, float[]>> out) throws Exception {
                    float[] sum = new float[numFactors];
                    long count = 0L;
                    Arrays.fill(sum, 0.F);

                    for (Tuple2 <Integer, float[]> factors : values) {
                        for (int j = 0; j < numFactors; j++) {
                            sum[j] += factors.f1[j];
                        }
                        count++;
                    }
                    out.collect(new Tuple2 <>(count, sum));
                }
            })
            .mapPartition(new MapPartitionFunction <Tuple2 <Long, float[]>, float[]>() {
                @Override
                public void mapPartition(Iterable <Tuple2 <Long, float[]>> values, Collector <float[]> out) throws
                    Exception {
                    float[] sum = new float[numFactors];
                    long count = 0L;
                    Arrays.fill(sum, 0.F);

                    for (Tuple2 <Long, float[]> value : values) {
                        for (int i = 0; i < numFactors; i++) {
                            sum[i] += value.f1[i];
                        }
                        count += value.f0;
                    }

                    for (int i = 0; i < numFactors; i++) {
                        sum[i] /= count;
                    }
                    out.collect(sum);
                }
            })
            .setParallelism(1);

        return reducedFactors;
    }

    public DataSet <String> getSerializedFactors(final String who) {
        DataSet <Tuple2 <Integer, float[]>> factorBlock = who.equals("user") ? model.userFactors : model.itemFactors;

        DataSet <String> serialized = factorBlock.
            map(new MapFunction <Tuple2 <Integer, float[]>, String>() {
                @Override
                public String map(Tuple2 <Integer, float[]> value) throws Exception {
                    // encode: who:factors:id
                    StringBuilder sbd = new StringBuilder();
                    // the flag indicating user or item
                    sbd.append(who);
                    sbd.append(":");
                    // the factors
                    for (int j = 0; j < value.f1.length; j++) {
                        sbd.append(Float.toString(value.f1[j]));
                        sbd.append(":");
                    }
                    // the id
                    sbd.append(value.f0);
                    return sbd.toString();
                }
            });

        DataSet <float[]> avgFactors = who.equals("user") ? model.userAvgFactors : model.itemAvgFactors;

        if (avgFactors != null) {
            DataSet <String> avgSerialized = avgFactors.map(new MapFunction <float[], String>() {
                @Override
                public String map(float[] factors) throws Exception {
                    StringBuilder sbd = new StringBuilder();
                    sbd.append("avg_" + who);
                    sbd.append(":");
                    // the factors
                    for (int i = 0; i < factors.length; i++) {
                        sbd.append(Float.toString(factors[i]));
                        sbd.append(":");
                    }
                    // the id
                    sbd.append(-1L);
                    return sbd.toString();
                }
            });

            serialized = serialized.union(avgSerialized);
        }

        return serialized;
    }

    private static class UpdateFactorExplicitOp extends RichCoGroupFunction <
        Tuple5 <Integer, Integer, int[], float[], float[]>,
        Tuple4 <Integer, Integer, Integer, float[]>,
        Tuple5 <Integer, Integer, int[], float[], float[]>> {

        private int src;
        private int dst;

        private int numFactors;
        private double lambda;

        public UpdateFactorExplicitOp(int numFactors, double lambda, int src, int dst) {
            this.numFactors = numFactors;
            this.lambda = lambda;
            this.src = src;
            this.dst = dst;
        }

        /**
         * @param srcStat:       tuple5: srcBlockId, srcNodeId, srcNb, srcRating, srcFactors
         * @param dstFactors:    tuple4: srcBlockId, dstBlockId, dstNodeId, dstFactors
         * @param updatedSrcStat
         * @throws Exception
         */
        @Override
        public void coGroup(Iterable <Tuple5 <Integer, Integer, int[], float[], float[]>> srcStat,
                            Iterable <Tuple4 <Integer, Integer, Integer, float[]>> dstFactors,
                            Collector <Tuple5 <Integer, Integer, int[], float[], float[]>> updatedSrcStat)
            throws Exception {

            if (null == srcStat) { return; }

            try {
                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                int stepNo = getIterationRuntimeContext().getSuperstepNumber();
                System.out.println("[" + taskId + "]: " + "ALS iteration step " + stepNo);
            } catch (Exception e) {
                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("[" + taskId + "]: " + "ALS iteration");
            }

            // buffer the dst factors, which is already sorted
            List <Tuple4 <Integer, Integer, Integer, float[]>> buffer = new ArrayList <>();
            for (Tuple4 <Integer, Integer, Integer, float[]> factor : dstFactors) { buffer.add(factor); }

            int[] dstNodeIds = new int[buffer.size()];
            for (int i = 0; i < buffer.size(); i++) {
                dstNodeIds[i] = buffer.get(i).f2;
            }

            NormalEquation ls = new NormalEquation(numFactors);
            float[] srcFactor;

            // loop over each src node
            for (Tuple5 <Integer, Integer, int[], float[], float[]> srcNode : srcStat) {
                ls.reset();

                int numExplicit = 0;
                final int[] nb = srcNode.f2;
                final float[] ra = srcNode.f3;
                for (int i = 0; i < nb.length; i++) {
                    int dstNodeId = nb[i];
                    float rating = ra[i];
                    int pos = Arrays.binarySearch(dstNodeIds, dstNodeId);
                    if (pos < 0) {
                        throw new RuntimeException("unexpected");
                    }
                    float[] dstFactor = buffer.get(pos).f3;

                    ls.add(dstFactor, (double)rating, 1.0);
                    numExplicit += 1;
                }

                srcFactor = LeastSquaresNESolver.solve(ls, numExplicit * lambda);
                updatedSrcStat.collect(new Tuple5 <>(srcNode.f0, srcNode.f1, srcNode.f2, srcNode.f3, srcFactor));
            }
        }
    }

    private static class UpdateFactorImplicitOp extends RichCoGroupFunction <
        Tuple5 <Integer, Integer, int[], float[], float[]>,
        Tuple4 <Integer, Integer, Integer, float[]>,
        Tuple5 <Integer, Integer, int[], float[], float[]>> {

        private double alpha;
        private double[][] YtY;

        private int numFactors;
        private double lambda;

        private int src;
        private int dst;

        public UpdateFactorImplicitOp(int numFactors, double lambda, double alpha, int src, int dst) {
            this.numFactors = numFactors;
            this.lambda = lambda;
            this.alpha = alpha;
            this.src = src;
            this.dst = dst;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            List <double[][]> YtYInList = getRuntimeContext().getBroadcastVariable("YtY");
            this.YtY = YtYInList.get(0);
        }

        @Override
        public void coGroup(Iterable <Tuple5 <Integer, Integer, int[], float[], float[]>> srcStat,
                            Iterable <Tuple4 <Integer, Integer, Integer, float[]>> dstFactors,
                            Collector <Tuple5 <Integer, Integer, int[], float[], float[]>> updatedSrcStat)
            throws Exception {

            if (null == srcStat) { return; }

            List <Tuple4 <Integer, Integer, Integer, float[]>> buffer = new ArrayList <>();

            // buffer the dst factors, which is already sorted
            for (Tuple4 <Integer, Integer, Integer, float[]> factor : dstFactors) { buffer.add(factor); }

            int[] dstNodeIds = new int[buffer.size()];
            for (int i = 0; i < buffer.size(); i++) {
                dstNodeIds[i] = buffer.get(i).f2;
            }

            NormalEquation ls = new NormalEquation(numFactors);
            float[] srcFactor;

            // loop over each src node
            for (Tuple5 <Integer, Integer, int[], float[], float[]> srcNode : srcStat) {
                if (src == 1 && srcNode.f1 == 0) {
                    System.out.println("ALS iteration step " + getIterationRuntimeContext().getSuperstepNumber());
                }

                ls.reset();

                // put the YtY
                ls.merge(this.YtY);

                int numExplicit = 0;
                final int[] nb = srcNode.f2;
                final float[] ra = srcNode.f3;
                for (int i = 0; i < nb.length; i++) {
                    int dstNodeId = nb[i];
                    float rating = ra[i];
                    int pos = Arrays.binarySearch(dstNodeIds, dstNodeId);
                    if (pos < 0) {
                        throw new RuntimeException("unexpected");
                    }
                    float[] dstFactor = buffer.get(pos).f3;

                    // Extension to the original paper to handle rating < 0. confidence is a function
                    // of |rating| instead so that it is never negative. c1 is confidence - 1.
                    double c1 = alpha * Math.abs(rating);
                    // For rating <= 0, the corresponding preference is 0. So the second argument of put
                    // is only there for rating > 0.
                    if (rating > 0.0) {
                        numExplicit += 1;
                    }
                    ls.add(dstFactor, ((rating > 0.0) ? (1.0 + c1) : 0.0), c1);
                }

                srcFactor = LeastSquaresNESolver.solve(ls, numExplicit * lambda);
                updatedSrcStat.collect(new Tuple5 <>(srcNode.f0, srcNode.f1, srcNode.f2, srcNode.f3, srcFactor));
            }
        }
    }
}
