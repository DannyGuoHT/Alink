package com.alibaba.alink.batchoperator.nlp;

import java.util.Arrays;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;

import org.apache.flink.types.Row;
import org.junit.Test;

public class TfidfBatchOpTest {
    @Test
    public void test() throws Exception {
        //docment array
        Row[] array2 = new Row[] {
            Row.of(new Object[] {"doc0", "中国", 1L}),
            Row.of(new Object[] {"doc0", "的", 1L}),
            Row.of(new Object[] {"doc0", "文化", 1L}),
            Row.of(new Object[] {"doc1", "只要", 1L}),
            Row.of(new Object[] {"doc1", "功夫", 1L}),
            Row.of(new Object[] {"doc1", "深", 1L}),
            Row.of(new Object[] {"doc2", "北京", 1L}),
            Row.of(new Object[] {"doc2", "的", 1L}),
            Row.of(new Object[] {"doc2", "拆迁", 1L}),
            Row.of(new Object[] {"doc3", "人名", 1L}),
            Row.of(new Object[] {"doc3", "的", 1L}),
            Row.of(new Object[] {"doc3", "名义", 1L})
        };
        AlinkParameter parameter = new AlinkParameter();
        parameter.put(ParamName.docIdColName, "docid");
        parameter.put("sentenceColName", "content");
        parameter.put("contentColName", "content");
        parameter.put("n", 2);
        parameter.put("wordColName", "word");
        parameter.put("countColName", "cnt");
        // create tf idf batch operator
        TfidfBatchOp tfidf = new TfidfBatchOp(parameter);
        //Generate MemSourceBatchOp via array2
        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array2), new String[] {"docid", "word", "cnt"});

        //Print tf idf result
        tfidf.linkFrom(
            data
        ).print();

    }

}