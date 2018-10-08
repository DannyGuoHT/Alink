package com.alibaba.alink.streamoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamDefaultValue;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.nlp.TokenizeUDF;
import com.alibaba.alink.streamoperator.StreamOperator;

public class TokenizeStreamOp extends StreamOperator {

    public TokenizeStreamOp(String selectColName, String outputColName) {
        this(selectColName, outputColName, null);
    }

    public TokenizeStreamOp(String selectColName, String outputColName, String wordDelimiter) {
        this(new AlinkParameter()
                .put(ParamName.selectedColName, selectColName)
                .put(ParamName.outputColName, outputColName)
                .putIgnoreNull(ParamName.wordDelimiter, wordDelimiter)
        );
    }

    public TokenizeStreamOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {

        this.table = in.udf(
                this.params.getString(ParamName.selectedColName),
                this.params.getStringOrDefault(ParamName.outputColName, null),
                new TokenizeUDF(this.params.getStringOrDefault(ParamName.wordDelimiter, ParamDefaultValue.WordDelimiter)
                        , params.getBoolOrDefault("toLowerCase", false))
                , this.params.getStringArrayOrDefault(ParamName.keepColNames, null)
        ).getTable();
        return this;

    }
}
