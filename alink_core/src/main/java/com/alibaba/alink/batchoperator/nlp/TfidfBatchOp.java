package com.alibaba.alink.batchoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import static org.apache.calcite.runtime.SqlFunctions.ln;

/*
*The implementation of TFIDF based on the result of doc word count algorithm
* we use table api to implement the algorithm
*/

public class TfidfBatchOp extends BatchOperator {

    /**
     * Default Constructor for TFIDF batch operator
     */
    public TfidfBatchOp(){
        super(null);
    }

    /**
     * Constructor for TFIDF batch operator
     * @param docIdColName doc id column name
     * @param wordColName word column name
     * @param countColName count column name
     */
    public TfidfBatchOp(String docIdColName, String wordColName, String countColName) {
        super(null);
        setDocIdColName(docIdColName);
        setWordColName(wordColName);
        setCountColName(countColName);
    }
    /**
     * Constructor for TFIDF batch operator
     * @param parameters alink parameter name
     */
    public TfidfBatchOp(AlinkParameter parameters) {
        super(parameters);
    }
    /*
    * Set docIdColName
     */
    public TfidfBatchOp setDocIdColName(String docIdColName){
        putParamValue(ParamName.docIdColName,docIdColName);
        return this;
    }

    /*
     * Set wordColName
     */
    public TfidfBatchOp setWordColName(String wordColName){
        putParamValue("wordColName",wordColName);
        return this;
    }

    /*
     * Set countColName
     */
    public TfidfBatchOp setCountColName(String countColName){
        putParamValue("countColName",countColName);
        return this;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String wordColName = params.getString("wordColName");
        String docIdColName = params.getString(ParamName.docIdColName);
        String countColName = params.getString("countColName");

        // Count doc and word count in a doc
        final BatchOperator doc_stat = in.groupBy(docIdColName, docIdColName + ",sum(" + countColName + ") as total_word_count");
        //Count totoal word count of words
        BatchOperator word_stat = in.groupBy(wordColName + "," + docIdColName, wordColName + "," + docIdColName + ",COUNT(1 ) as tmp_count")
                .groupBy(wordColName, wordColName + ",count(1) as doc_cnt");

        final String tmp_col_names = docIdColName + "," + wordColName + "," + countColName + "," + "total_word_count";
        final String tmp_col_names1 = tmp_col_names + ",doc_cnt";
        final int docIdIndex = TableUtil.findIndexFromName(in.getColNames(), docIdColName);
        final int wordIndex = TableUtil.findIndexFromName(in.getColNames(), wordColName);
        final int wordCntIndex = TableUtil.findIndexFromName(in.getColNames(), countColName);
        String tmp_col_names2 = tmp_col_names1 + ",total_doc_count,tf,idf,tfidf";

        //Count tf idf resulst of words in docs
        this.setTable(in.join(doc_stat.as("docid1,total_word_count"), docIdColName + " = docid1", "1 as id1," + tmp_col_names)
                .join(word_stat.as("word1,doc_cnt"), wordColName + " = " + "word1", "id1," + tmp_col_names1)
                .getDataSet()
                .join(doc_stat.select("1 as id,count(1) as total_doc_count").getDataSet()
                        , JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where("id1").equalTo("id")
                .map(new MapFunction<Tuple2<Row, Row>, Row>() {
                    @Override
                    public Row map(Tuple2<Row, Row> rowRowTuple2) throws Exception {
                        Row row = new Row(9);
                        String docId = rowRowTuple2.f0.getField(1).toString();
                        String word = rowRowTuple2.f0.getField(2).toString();
                        Long wordCount = (Long) rowRowTuple2.f0.getField(3);
                        Long totalWordCount = (Long) rowRowTuple2.f0.getField(4);
                        Long doc_count = (Long) rowRowTuple2.f0.getField(5);
                        Long total_doc_count = (Long) rowRowTuple2.f1.getField(1);
                        double tf = 1.0 * wordCount / total_doc_count;
                        double idf = ln(1.0 * total_doc_count / doc_count);
                        row.setField(0, docId);
                        row.setField(1, word);
                        row.setField(2, wordCount);
                        row.setField(3, totalWordCount);
                        row.setField(4, doc_count);
                        row.setField(5, total_doc_count);
                        row.setField(6, tf);
                        row.setField(7, idf);
                        row.setField(8, tf * idf);
                        return row;
                    }
                }), tmp_col_names2.split(","), new TypeInformation<?>[]{Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE});
        ;
        return this;
    }
}
