package com.alibaba.alink.batchoperator.ml.classification;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.ml.classification.NaiveBayesPredictStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;


public class NaiveBayesTrainBatchOpTest {
    @Test
    public void testBatchTable() throws Exception {
        AlinkSession.getExecutionEnvironment().getConfig().disableSysoutLogging();
        AlinkSession.getExecutionEnvironment().setParallelism(4);

        Row[] array = new Row[] {
            Row.of(new Object[] {1.0, 1.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 1.0, 0.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 0.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 0.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0})
        };

        Row[] array_pre = new Row[] {
            Row.of(new Object[] {1.0, 1.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 1.0, 0.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 0.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {1.0, 0.0, 1.0, 1.0, 1.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0}),
            Row.of(new Object[] {0.0, 1.0, 1.0, 0.0, 0.0})
        };
        /* load training data */
        MemSourceBatchOp data = new MemSourceBatchOp(
            Arrays.asList(array), new String[] {"f0", "f1", "f2", "f3", "labels"});
        MemSourceBatchOp data_pred = new MemSourceBatchOp(Arrays.asList(array_pre),
            new String[] {"labels", "f1", "f2", "f3", "f0"});

        String[] featNames = new String[] {"f0", "f1", "f2", "f3"};
        String labelName = "labels";

        /* train model */
        NaiveBayesTrainBatchOp nb
            = new NaiveBayesTrainBatchOp()
            .setBayesType("Bernoulli")
            .setLabelColName(labelName)
            .setFeatureColNames(featNames)
            .setSmoothing(0.5);

        BatchOperator bmodel = data.link(nb);

        /* predict */
        NaiveBayesPredictBatchOp predictor = new NaiveBayesPredictBatchOp()
            .setPredDetailColName("predResultColName");

        List <Row> testResult = predictor.linkFrom(bmodel, data_pred).getDataSet().collect();

        /* assert the result */
        double[] labels = new double[testResult.size()];
        double[] preds = new double[testResult.size()];
        int iter = 0;
        for (Row row : testResult) {
            labels[iter] = (double)row.getField(4);
            preds[iter++] = (double)row.getField(5);

        }
        Assert.assertArrayEquals(labels, preds, 1.0e-30);
        System.out.println("Naive Bayes algo. table test OK.");
    }

    @Test
    public void testBatchTensor() throws Exception {
        AlinkSession.getExecutionEnvironment().getConfig().disableSysoutLogging();
        AlinkSession.getExecutionEnvironment().setParallelism(4);

        Row[] array = new Row[] {
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:0.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:0.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:0.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0})
        };

        /* load training data */
        MemSourceBatchOp data = new MemSourceBatchOp(
            Arrays.asList(array), new String[] {"tensor", "labels"});
        MemSourceBatchOp data_pred = data;

        String labelName = "labels";

        /* train model */
        NaiveBayesTrainBatchOp nb
            = new NaiveBayesTrainBatchOp()
            .setBayesType("Bernoulli")
            .setLabelColName(labelName)
            .setTensorColName("tensor")
            .setIsSparse(true)
            .setSmoothing(0.5);

        BatchOperator bmodel = data.link(nb);

        /* predict */
        NaiveBayesPredictBatchOp predictor = new NaiveBayesPredictBatchOp()
            .setPredDetailColName("predResultColName")
            .setTensorColName("tensor");

        List <Row> testResult = predictor.linkFrom(bmodel, data_pred).getDataSet().collect();

        /* assert the result */
        double[] labels = new double[testResult.size()];
        double[] preds = new double[testResult.size()];
        int iter = 0;
        for (Row row : testResult) {
            labels[iter] = (double)row.getField(1);
            preds[iter++] = (double)row.getField(2);

        }
        Assert.assertArrayEquals(labels, preds, 1.0e-30);
        System.out.println("Naive Bayes algo. tensor test OK.");
    }

    @Test
    public void testStreamTensor() throws Exception {
        AlinkSession.getExecutionEnvironment().getConfig().disableSysoutLogging();
        AlinkSession.getExecutionEnvironment().setParallelism(4);

        Row[] array = new Row[] {
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:1.0, 2:0.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:0.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:1.0, 1:0.0, 2:1.0, 30:1.0", 1.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0}),
            Row.of(new Object[] {"0:0.0, 1:1.0, 2:1.0, 30:0.0", 0.0})
        };

        /* load training data */
        MemSourceBatchOp data = new MemSourceBatchOp(
            Arrays.asList(array), new String[] {"tensor", "labels"});
        MemSourceStreamOp data_pred = new MemSourceStreamOp(
            Arrays.asList(array), new String[] {"tensor", "labels"});

        String labelName = "labels";

        /* train model */
        NaiveBayesTrainBatchOp nb
            = new NaiveBayesTrainBatchOp()
            .setBayesType("Bernoulli")
            .setLabelColName(labelName)
            .setTensorColName("tensor")
            .setIsSparse(true)
            .setSmoothing(0.5);

        BatchOperator bmodel = data.link(nb);

        /* predict */
        NaiveBayesPredictStreamOp predictor = new NaiveBayesPredictStreamOp(bmodel)
            .setPredDetailColName("predResultColName")
            .setTensorColName("tensor");

        predictor.linkFrom(data_pred).print();
        StreamOperator.execute();
    }
}