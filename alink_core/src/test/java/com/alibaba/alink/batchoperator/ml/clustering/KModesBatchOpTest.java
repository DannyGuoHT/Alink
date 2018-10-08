package com.alibaba.alink.batchoperator.ml.clustering;

import java.util.Random;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.constants.ClusterConstant;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.ml.clustering.KModesPredictStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KModesBatchOpTest {

    private MemSourceBatchOp inputTableBatch;
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

        String[] alphaTable = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"};

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
                objs[i][j + 1] = alphaTable[random.nextInt(12)];
            }
        }

        inputTableBatch = new MemSourceBatchOp(objs, colNames);
        predictTableBatch = new MemSourceBatchOp(objs, colNames);
        predictTableStream = new MemSourceStreamOp(objs, colNames);

    }

    @Test
    public void testTable() throws Exception {
        // kModes batch
        KModesBatchOp kModesBatchOp = new KModesBatchOp()
            .setK(k)
            .setFeatureColNames(featureColNames)
            .setNumIter(10)
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kModesBatchOp.linkFrom(inputTableBatch).print();
        kModesBatchOp.getSideOutput(0).print();

        // kModes batch predict
        KModesPredictBatchOp kModesPredictBatchOp = new KModesPredictBatchOp()
            .setKeepColNames(new String[] {"id"})
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kModesPredictBatchOp.linkFrom(kModesBatchOp.getSideOutput(0), predictTableBatch).print();

        // kModes stream predict
        KModesPredictStreamOp kModesPredictStreamOp = new KModesPredictStreamOp(kModesBatchOp.getSideOutput(0))
            .setKeepColNames(new String[] {"id"})
            .setPredResultColName(ClusterConstant.PRED_RESULT_COL_NAME);
        kModesPredictStreamOp.linkFrom(predictTableStream).print();
        StreamOperator.execute();

        int modelLineNumber = kModesBatchOp.getSideOutput(0).collect().size();
        Assert.assertArrayEquals(new int[] {modelLineNumber}, new int[] {k + 1});
    }
}
