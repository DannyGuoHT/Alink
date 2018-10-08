package com.alibaba.alink.streamoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.nlp.OutputTableInfo;
import com.alibaba.alink.common.nlp.StringSimilarityConst;
import com.alibaba.alink.common.nlp.StringSimilarityWindowFunction;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Calculate the similarity between characters in pair.
 */

public class StringSimilarityStreamOp extends StreamOperator implements Serializable {
    /**
     * default constructor
     */
    public StringSimilarityStreamOp(){
        super(null);
    }

    public StringSimilarityStreamOp(AlinkParameter params) {
        super(params);
    }

    /**
     * constructor.
     * @param selectedColName0 the first string col
     * @param selectedColName1 the second string col
     * @param method similarity method
     * @param k window size for SSK, COSINE and SIMHASH
     * @param lambda parameter for SSK
     * @param seed random seed for MINHASH
     * @param minHashK parameter for MINHASH
     * @param bucket parameter for MINHASH
     * @param timeInterval stream window size
     */
    public StringSimilarityStreamOp(String selectedColName0, String selectedColName1, String method,
                                    Integer k, Double lambda, Integer seed, Integer minHashK,
                                    Integer bucket, Integer timeInterval) {
        this(new AlinkParameter()

                .put("selectedColName0", selectedColName0)
                .put("selectedColName1", selectedColName1)
                .put("method", method)
                .put("k", k)
                .put("lambda", lambda)
                .put("seed", seed)
                .put("minHashK", minHashK)
                .put("bucket", bucket)
                .put("timeInterval", timeInterval));
    }

    public StringSimilarityStreamOp setSelectedColName0(String value){
        params.putIgnoreNull("selectedColName0", value);
        return this;
    }

    public StringSimilarityStreamOp setSelectedColName1(String value){
        params.putIgnoreNull("selectedColName1", value);
        return this;
    }

    public StringSimilarityStreamOp setMethod(String value){
        params.putIgnoreNull("method", value);
        return this;
    }


    public StringSimilarityStreamOp setK(Integer value){
        params.putIgnoreNull("k", value);
        return this;
    }

    public StringSimilarityStreamOp setLambda(Double value){
        params.putIgnoreNull("lambda", value);
        return this;
    }

    public StringSimilarityStreamOp setSeed(Integer value){
        params.putIgnoreNull("seed", value);
        return this;
    }

    public StringSimilarityStreamOp setMinHashK(Integer value){
        params.putIgnoreNull("minHashK", value);
        return this;
    }

    public StringSimilarityStreamOp setBucket(Integer value){
        params.putIgnoreNull("bucket", value);
        return this;
    }

    public StringSimilarityStreamOp setOuputColName(String value){
        params.putIgnoreNull(ParamName.outputColName, value);
        return this;
    }

    public StringSimilarityStreamOp setKeepColNames(String[] value){
        params.putIgnoreNull(ParamName.keepColNames, value);
        return this;
    }

    public StringSimilarityStreamOp setTimeInterval(Integer value){
        params.putIgnoreNull("timeInterval", value);
        return this;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        String selectedColName0 = this.params.getString("selectedColName0");
        String selectedColName1 = this.params.getString("selectedColName1");
        String outputColName = this.params.getStringOrDefault(ParamName.outputColName, StringSimilarityConst.OUTPUT);
        String[] keepColNames = this.params.getStringArrayOrDefault(ParamName.keepColNames, StringSimilarityConst.KEEPCOLNAMES);
        Integer timeInterval = this.params.getIntegerOrDefault("timeInterval", StringSimilarityConst.INTERVAL);

        OutputTableInfo outTable = new OutputTableInfo();
        outTable.getOutputTableInfo(in.getColNames(), in.getColTypes(), selectedColName0, selectedColName1, outputColName, keepColNames);

        StringSimilarityWindowFunction ss = new StringSimilarityWindowFunction(this.params, false);

        DataStream<Row> out = in.select(outTable.getSelectedColNames()).getDataStream()
                .timeWindowAll(Time.of(timeInterval, TimeUnit.SECONDS))
                .apply(ss);

        this.table = RowTypeDataStream.toTable(out, outTable.getOutColNames(), outTable.getOutTypes());

        return this;
    }
}
