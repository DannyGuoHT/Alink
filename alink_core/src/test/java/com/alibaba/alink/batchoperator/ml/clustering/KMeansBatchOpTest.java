package com.alibaba.alink.batchoperator.ml.clustering;

import java.util.Arrays;
import java.util.Random;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ClusterConstant;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.clustering.DistanceType;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.ml.clustering.KMeansPredictStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KMeansBatchOpTest {

    private MemSourceBatchOp inputTableBatch;
    private MemSourceBatchOp initialCenterTableBatch;
    private MemSourceBatchOp predictTableBatch;
    private MemSourceStreamOp predictTableStream;
    private String[] featureColNames;

    private int k = 10;

    @Before
    public void setUp() throws Exception {
        genTable();
    }

    void genTable() {
        //data
        int row = 100;
        int col = 10;
        Random random = new Random(2018L);

        Object[][] objs = new Object[row][col + 1];
        String[] colNames = new String[col + 1];
        featureColNames = new String[col];
        colNames[0] = "id";

        for (int i = 0; i < col; i++) {
            colNames[i + 1] = "col" + i;
            featureColNames[i] = colNames[i + 1];
        }

        for (int i = 0; i < row; i++) {
            objs[i][0] = (long)i;
            for (int j = 0; j < col; j++) {
                objs[i][j + 1] = random.nextDouble();
            }
        }

        inputTableBatch = new MemSourceBatchOp(objs, colNames);
        predictTableBatch = new MemSourceBatchOp(objs, colNames);
        predictTableStream = new MemSourceStreamOp(objs, colNames);

        Object[][] initialCenter = new Object[k][col + 1];
        for (int i = 0; i < 10; i++) {
            initialCenter[i] = objs[i];
        }
        colNames[0] = ClusterConstant.PRED_RESULT_COL_NAME;
        initialCenterTableBatch = new MemSourceBatchOp(initialCenter, colNames);
    }

    @Test
    public void test() throws Exception {
        String[] featureNames = new String[] {"X", "Y"};

        Row[] testArray =
            new Row[] {
                Row.of(new Object[] {0, 0}),
                Row.of(new Object[] {8, 8}),
                Row.of(new Object[] {1, 2}),
                Row.of(new Object[] {9, 10}),
                Row.of(new Object[] {3, 1}),
                Row.of(new Object[] {10, 7})
            };

        MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), featureNames);

        KMeansBatchOp kmb = new KMeansBatchOp(new AlinkParameter().
            put(ParamName.featureColNames, featureNames).
            put(ParamName.k, 2).
            put(ParamName.predResultColName, ClusterConstant.PRED_RESULT_COL_NAME));

        kmb.linkFrom(inOp).print();
    }

    @Test
    public void testTable() throws Exception {
        // kMeans batch
        KMeansBatchOp kMeansBatchOp = new KMeansBatchOp()
            .setK(k)
            .setDistanceType(DistanceType.EUCLIDEAN)
            .setFeatureColNames(featureColNames)
            .setNumIter(10)
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kMeansBatchOp.linkFrom(inputTableBatch).print();
        kMeansBatchOp.linkFrom(inputTableBatch, initialCenterTableBatch).print();
        kMeansBatchOp.getSideOutput(0).print();

        // kMeans batch predict
        KMeansPredictBatchOp kMeansPredictBatchOp = new KMeansPredictBatchOp()
            .setKeepColNames(new String[] {"id"})
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kMeansPredictBatchOp.linkFrom(kMeansBatchOp.getSideOutput(0), predictTableBatch).print();

        // kMeans stream predict
        KMeansPredictStreamOp kMeansPredictStreamOp = new KMeansPredictStreamOp(kMeansBatchOp.getSideOutput(0))
            .setKeepColNames(new String[] {"id"})
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kMeansPredictStreamOp.linkFrom(predictTableStream).print();
        StreamOperator.execute();

        int modelLineNumber = kMeansBatchOp.getSideOutput(0).collect().size();
        Assert.assertArrayEquals(new int[] {modelLineNumber}, new int[] {k + 1});
    }
}
