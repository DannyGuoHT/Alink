package com.alibaba.alink.batchoperator.outlier;

import java.util.List;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.CsvSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class SosBatchOpTest {
    @Test
    public void test() throws Exception {
        String url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv";
        String schemaStr
            = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

        BatchOperator data = new CsvSourceBatchOp(new AlinkParameter()
            .put("filePath", url)
            .put("schemaStr", schemaStr));

        BatchOperator sos = new SosBatchOp()
            .setSelectedColNames(new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width"})
            .setPredResultColName("score");

        List <Row> result = data.link(sos).collect();
        Assert.assertTrue(result.size() > 0);
    }

}