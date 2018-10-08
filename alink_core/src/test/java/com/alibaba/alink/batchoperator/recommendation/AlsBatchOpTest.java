package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.CsvSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AlsBatchOpTest {
    @Test
    public void test() throws Exception {
        String url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv";
        String schemaStr = "userid bigint, movieid bigint, rating double, timestamp string";

        BatchOperator data = new CsvSourceBatchOp(new AlinkParameter()
                .put("filePath", url)
                .put("schemaStr", schemaStr));

        BatchOperator als = new AlsBatchOp()
                .setUserColName("userid")
                .setItemColName("movieid")
                .setRateColName("rating")
                .setPredResultColName("topk");

        List<Row> result = data.link(als).collect();
        Assert.assertTrue(result.size() > 0);
    }

}