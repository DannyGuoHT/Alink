package com.alibaba.alink.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class TokenizeUDF extends ScalarFunction {

    private static final String delim = "(?<=\\S)(?:(?<=\\p{Punct})|(?=\\p{Punct}))(?=\\S)";
    private final String wordDelimeter;
    private boolean toLowerCase = false;

    public TokenizeUDF(String wordDelimeter) {
        this(wordDelimeter, false);
    }

    public TokenizeUDF(String wordDelimeter, boolean toLowerCase) {
        this.wordDelimeter = wordDelimeter;
        this.toLowerCase = toLowerCase;
    }

    public String eval(String content) {
        if (toLowerCase) { content = content.toLowerCase(); }
        return content.replaceAll(delim, " ");
    }

    @Override
    public TypeInformation <?> getResultType(Class <?>[] signature) {
        return Types.STRING;
    }
}
