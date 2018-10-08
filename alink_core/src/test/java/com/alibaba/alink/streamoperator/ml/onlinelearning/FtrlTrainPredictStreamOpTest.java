package com.alibaba.alink.streamoperator.ml.onlinelearning;

import java.util.Arrays;

import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Test;

public class FtrlTrainPredictStreamOpTest {
    @Test
    public void FtrlClassification() throws Exception {
        Row[] array = new Row[] {
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 3:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 3:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 3:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 3:1.0", 1.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 3:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 3:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 3:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 3:0.0", 0.0})
        };

        /* load training data */
        MemSourceStreamOp data = new MemSourceStreamOp(
            Arrays.asList(array), new String[] {"tensor", "labels"});

        new FtrlTrainPredictStreamOp()
            .setTensorColName("tensor")
            .setLabelColName("labels")
            .setPredResultColName("pred")
            .setAlpha(0.1)
            .setBeta(0.1)
            .setL1(0.01)
            .setL2(0.1)
            .setMaxWaitTime(10000)
            .setSparseFeatureDim(1000000)
        .linkFrom(data, data).print();
        StreamOperator.execute();
    }
}