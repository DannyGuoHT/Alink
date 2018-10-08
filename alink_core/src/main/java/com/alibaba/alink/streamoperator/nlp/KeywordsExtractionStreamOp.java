package com.alibaba.alink.streamoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.nlp.KeywordsExtractionWindowFunction;
import com.alibaba.alink.common.nlp.TextRankConst;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * Automatically identify in a text a set of terms that best describe the document based on TextRank.
 */
public class KeywordsExtractionStreamOp extends StreamOperator {
    /**
     * default constructor.
     */
    public KeywordsExtractionStreamOp() {
        super(null);
    }

    public KeywordsExtractionStreamOp(AlinkParameter params) {
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
     * @param timeInterval stream window size.
     */
    public KeywordsExtractionStreamOp(String docContentColName, Integer topN, Integer windowSize,
                                      Double dampingFactor, Double epsilon, Integer maxIteration, Integer timeInterval) {
        this(new AlinkParameter()
            .put("docContentColName", docContentColName)
            .put("topN", topN)
            .put("windowSize", windowSize)
            .put("dampingFactor", dampingFactor)
            .put("maxIteration", maxIteration)
            .put("epsilon", epsilon)
            .put("timeInterval", timeInterval));
    }

    public KeywordsExtractionStreamOp setDocIdColName(String value){
        params.putIgnoreNull(ParamName.docIdColName, value);
        return this;
    }

    public KeywordsExtractionStreamOp setDocContentColName(String value){
        params.putIgnoreNull("docContentColName", value);
        return this;
    }

    public KeywordsExtractionStreamOp setTopN(Integer value){
        params.putIgnoreNull("topN", value);
        return this;
    }

    public KeywordsExtractionStreamOp setWindowSize(Integer value){
        params.putIgnoreNull("windowSize", value);
        return this;
    }

    public KeywordsExtractionStreamOp setDampingFactor(Double value){
        params.putIgnoreNull("dampingFactor", value);
        return this;
    }

    public KeywordsExtractionStreamOp setMaxIteration(Integer value){
        params.putIgnoreNull("maxIteration", value);
        return this;
    }

    public KeywordsExtractionStreamOp setEpsilon(Double value){
        params.putIgnoreNull("epsilon", value);
        return this;
    }

    public KeywordsExtractionStreamOp setTimeInterval(Integer value){
        params.putIgnoreNull("timeInterval", value);
        return this;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        String docIdColName = this.params.getString(ParamName.docIdColName);
        String docContentColName = this.params.getString("docContentColName");
        Integer timeInterval = this.params.getIntegerOrDefault("timeInterval", TextRankConst.TIMEINTERVAL);

        String[] colNames = in.getColNames();
        TypeInformation[] types = in.getColTypes();
        int indexId = TableUtil.findIndexFromName(colNames, docIdColName);
        int contentId = TableUtil.findIndexFromName(colNames, docContentColName);

        if (indexId < 0 || contentId < 0) {
            throw new RuntimeException("ColName not exists");
        }

        KeywordsExtractionWindowFunction ke = new KeywordsExtractionWindowFunction(this.params);
        DataStream<Row> res = in.select(docIdColName + ", " + docContentColName).getDataStream()
            .timeWindowAll(Time.of(timeInterval, TimeUnit.SECONDS))
            .apply(ke);

        this.table = RowTypeDataStream.toTable(res,
            new String[] {"docId", "keyWords", "weight"},
            new TypeInformation[] {types[indexId], Types.STRING, Types.DOUBLE});

        return this;
    }
}
