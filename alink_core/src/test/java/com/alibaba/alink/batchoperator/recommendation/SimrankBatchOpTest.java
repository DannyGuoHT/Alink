package com.alibaba.alink.batchoperator.recommendation;

import java.util.Arrays;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class SimrankBatchOpTest {
    @Test
    public void test() throws Exception {
        Row[] rows = new Row[] {
            Row.of(new Object[] {0, 1, 6}),
            Row.of(new Object[] {1, 2, 6}),
            Row.of(new Object[] {2, 3, 8}),
            Row.of(new Object[] {2, 0, 8}),
        };

        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"u", "i", "r"});

        BatchOperator simrank = new SimrankBatchOp()
            .setUserColName("u")
            .setItemColName("r")
            .setWeightColName("i")
            .setPredResultColName("topk");

        int n = data.link(simrank).collect().size();
        Assert.assertTrue(n > 0);
    }

}