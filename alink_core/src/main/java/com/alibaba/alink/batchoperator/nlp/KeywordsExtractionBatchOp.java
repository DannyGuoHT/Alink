package com.alibaba.alink.batchoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.nlp.TextRank;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Automatically identify in a text a set of terms that best describe the document based on TextRank.
 */

public class KeywordsExtractionBatchOp extends BatchOperator{
    /**
     * default constructor.
     */
    public KeywordsExtractionBatchOp(){
        super(null);
    }

    public KeywordsExtractionBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * constructor.
     * @param docContentColName document content col.
     * @param topN output the top n keywords.
     * @param windowSize co-occurence window size.
     * @param dampingFactor parameter for textrank.
     * @param epsilon parameter for textrank.
     * @param maxIteration parameter for textrank.
     */
    public KeywordsExtractionBatchOp(String docContentColName, Integer topN, Integer windowSize,
                                     Double dampingFactor, Double epsilon, Integer maxIteration) {
        this(new AlinkParameter()
            .put("docContentColName", docContentColName)
            .put("topN", topN)
            .put("windowSize", windowSize)
            .put("dampingFactor", dampingFactor)
            .put("maxIteration", maxIteration)
            .put("epsilon", epsilon));
    }

    /**
     * Set the document ID col name
     * @param value document id colname
     * @return
     */
    public KeywordsExtractionBatchOp setDocIdColName(String value){
        putParamValue(ParamName.docIdColName, value);
        return this;
    }

    /**
     * Set the document content col name
     * @param value document content colname
     * @return
     */
    public KeywordsExtractionBatchOp setDocContentColName(String value){
        putParamValue("docContentColName", value);
        return this;
    }

    /**
     * Set the number of output
     * @param value the number of keywords for output
     * @return
     */
    public KeywordsExtractionBatchOp setTopN(Integer value){
        putParamValue("topN", value);
        return this;
    }

    /**
     * Set the window size.
     * @param value window size.
     * @return
     */
    public KeywordsExtractionBatchOp setWindowSize(Integer value){
        putParamValue("windowSize", value);
        return this;
    }

    /**
     * Set the damping factor for textrank
     * @param value damping factor.
     * @return
     */
    public KeywordsExtractionBatchOp setDampingFactor(Double value){
        putParamValue("dampingFactor", value);
        return this;
    }

    /** Set the max iteration to converge
     * @param value max iteration.
     * @return
     */
    public KeywordsExtractionBatchOp setMaxIteration(Integer value){
        putParamValue("maxIteration", value);
        return this;
    }

    /**
     * Set the threshold.
     * @param value threhold.
     * @return
     */
    public KeywordsExtractionBatchOp setEpsilon(Double value){
        putParamValue("epsilon", value);
        return this;
    }

    /**
     * the link from data
     * @param in input operator, which contains the data.
     * @return
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String docIdColName = this.params.getString(ParamName.docIdColName);
        String docContentColName = this.params.getString("docContentColName");

        // Get the input data.
        String[] colNames = in.getColNames();
        TypeInformation[] types = in.getColTypes();

        int indexId = TableUtil.findIndexFromName(colNames, docIdColName);
        int contentId = TableUtil.findIndexFromName(colNames, docContentColName);

        if(indexId < 0){
            throw new RuntimeException(docIdColName + " not exists!");
        }
        if(contentId < 0){
            throw new RuntimeException(docContentColName + " not exists!");
        }
        DataSet<Row> data = in.select(docIdColName + ", " + docContentColName).getDataSet();
        // Initialize the TextRank class, which runs the text rank algorithm.
        final TextRank textRank = new TextRank(this.params);
        DataSet<Row> res = data.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public void flatMap(Row row, Collector<Row> collector) throws Exception {
                // For each row, apply the text rank algorithm to get the key words.
                Row[] out = textRank.getKeyWords(row);
                for(int i = 0; i < out.length; i++){
                    collector.collect(out[i]);
                }
            }
        });
        // Set the output into table.
        this.table = RowTypeDataSet.toTable(res,
            new String[]{"docId", "keyWords", "weight"},
            new TypeInformation[]{types[indexId], Types.STRING, Types.DOUBLE});

        return this;
    }
}
