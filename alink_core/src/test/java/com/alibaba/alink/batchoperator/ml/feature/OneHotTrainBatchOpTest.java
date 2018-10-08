package com.alibaba.alink.batchoperator.ml.feature;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OneHotTrainBatchOpTest {

    @Test
    public void batchTest() throws Exception {
        AlinkSession.getExecutionEnvironment().getConfig().disableSysoutLogging();
        AlinkSession.getExecutionEnvironment().setParallelism(2);

         /* prepare training data */
        Row[] array = new Row[]{
                Row.of(new Object[]{"doc0", "天", 4l}),
                Row.of(new Object[]{"doc0", "地", 5l}),
                Row.of(new Object[]{"doc0", "人", 1l}),
                Row.of(new Object[]{"doc1", null, 3l}),
                Row.of(new Object[]{null, "人", 2l}),
                Row.of(new Object[]{"doc1", "合", 4l}),
                Row.of(new Object[]{"doc1", "一", 4l}),
                Row.of(new Object[]{"doc2", "清", 3l}),
                Row.of(new Object[]{"doc2", "一", 2l}),
                Row.of(new Object[]{"doc2", "色", 2l})
        };
        TableSchema schema = new TableSchema(
                new String[]{"docid", "word", "cnt"},
                new TypeInformation<?>[]{Types.STRING(), Types.STRING(), Types.LONG()}
        );
        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array), schema);

        String[] binaryNames = new String[]{"docid", "word"};
        String[] reserveNames = new String[]{"cnt"};
        String[] keepColNames = new String[]{"word", "docid", "cnt"};

        /* train the model */
        OneHotTrainBatchOp one_hot = new OneHotTrainBatchOp()
            .setSelectedColNames(binaryNames)
            .setReserveColNames(reserveNames)
            .setDropLast(false)
            .setIgnoreNull(false);
        BatchOperator model = data.link(one_hot);

        Row[] parray = new Row[]{
            Row.of(new Object[]{"doc0", "天", 4l}),
            Row.of(new Object[]{"doc2", null, 3l})
        };

        MemSourceBatchOp pred_data = new MemSourceBatchOp(Arrays.asList(parray), schema);

        /* batch predict */
        OneHotPredictBatchOp predictor = new OneHotPredictBatchOp("predicted", keepColNames);
        List<Row> results =
            predictor.linkFrom(model, pred_data).collect();

        /* assert the result. */
        for (Row row : results) {
            String docid = (String) row.getField(0);
            String word = (String) row.getField(1);
            String res = (String) row.getField(3);

            if (docid.equals("doc0") && word.equals("天")) {
                Assert.assertArrayEquals(new String[]{res}, new String[]{"0:4,4:1,10:1"});
                System.out.println("sample 1 test OK!");
            } else {
                Assert.assertArrayEquals(new String[]{res}, new String[]{"0:3,2:1,7:1"});
                System.out.println("sample 2 test OK!");
            }
        }
    }
}