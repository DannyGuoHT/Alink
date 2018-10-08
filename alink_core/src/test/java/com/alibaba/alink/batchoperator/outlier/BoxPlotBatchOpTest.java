package com.alibaba.alink.batchoperator.outlier;

import java.util.Arrays;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.outlier.BoxPlotPredictStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class BoxPlotBatchOpTest {
    MemSourceBatchOp memSourceBatchOp;
    MemSourceStreamOp memSourceStreamOp;

    @Before
    public void setUp() throws Exception {
        ExecutionConfig executionConfig = AlinkSession.getExecutionEnvironment().getConfig();
        executionConfig.disableSysoutLogging();
        Row[] array = new Row[] {
            Row.of(new Object[] {93.444, 1.4}),
            Row.of(new Object[] {93.2, 1.4}),
            Row.of(new Object[] {93.48, 1.2}),
            Row.of(new Object[] {93.444, 1.4}),
            Row.of(new Object[] {93.444, 1.8}),
            Row.of(new Object[] {82.444, -2.3})

        };

        memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(array), new String[] {"cons_price_rate", "emp_var_rate"});
        memSourceStreamOp = new MemSourceStreamOp(Arrays.asList(array),
            new String[] {"emp_var_rate", "cons_price_rate"});
    }


    @Test
    public void test() throws Exception {
        // Creat a Box Plot batchOp
        BoxPlotBatchOp boxPlotBatchOp = new BoxPlotBatchOp(new String[] {"cons_price_rate", "emp_var_rate"});
        String[] featureColNames = new String[] {"cons_price_rate", "emp_var_rate"};
        AlinkParameter parameter = new AlinkParameter();
        parameter.put(ParamName.featureColNames, featureColNames);
        //Train Box Plot Model
        boxPlotBatchOp.linkFrom(memSourceBatchOp);
        boxPlotBatchOp.print();
        // Creat a Box Plot Predict batch Operator
        BoxPlotPredictBatchOp predictor = new BoxPlotPredictBatchOp();
        predictor.setPredResultColName("outlierScore");
        //Predict result with trained box plot model
        DataSet <Row> testResult = predictor.linkFrom(boxPlotBatchOp, memSourceBatchOp).getDataSet();
        testResult.print();
        parameter.put("predResultColName", "outlierScore");

        // Creat a Box Plot Predict Stream Operator
        BoxPlotPredictStreamOp pred2 = new BoxPlotPredictStreamOp(boxPlotBatchOp, "outlierScore");
        memSourceStreamOp.link(pred2).print();

        StreamOperator.execute();
    }

}
