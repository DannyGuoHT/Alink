package com.alibaba.alink.batchoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamDefaultValue;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.nlp.TokenizeUDF;

public class TokenizeBatchOp extends BatchOperator {

    public TokenizeBatchOp(String selectColName, String outputColName) {
        this(selectColName, outputColName, " ");
    }

    public TokenizeBatchOp(String selectColName, String outputColName, String wordDelimiter) {
        this(new AlinkParameter()
                .put(ParamName.selectedColName, selectColName)
                .put(ParamName.outputColName, outputColName)
                .putIgnoreNull(ParamName.wordDelimiter, wordDelimiter)
                .put("toLowerCase", "false")

        );
    }

    public TokenizeBatchOp(String selectColName, String outputColName, String wordDelimiter, boolean toLowerCase) {

        this(new AlinkParameter()
                .put(ParamName.selectedColName, selectColName)
                .put(ParamName.outputColName, outputColName)
                .putIgnoreNull(ParamName.wordDelimiter, wordDelimiter)
                .put("toLowerCase", toLowerCase)
        );
    }

    public TokenizeBatchOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

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
