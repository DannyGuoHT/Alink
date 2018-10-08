package com.alibaba.alink.common.nlp;

import java.util.HashSet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class FilterStopUDF extends ScalarFunction {

    private HashSet <String> stopWordsDict;
    private final String wordDelimeter;

    public FilterStopUDF(HashSet <String> StopWordsDict, String wordDelimeter) {
        this.wordDelimeter = wordDelimeter;
        this.stopWordsDict = StopWordsDict;

    }

    public String eval(String content) {

        String[] tokens = content.split(wordDelimeter);
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (stopWordsDict.contains(token)) { continue; }
            if (i == tokens.length - 1) { sbd.append(token); } else { sbd.append(token).append(wordDelimeter); }
        }

        return sbd.toString();
    }

    @Override
    public TypeInformation <?> getResultType(Class <?>[] signature) {
        return Types.STRING;
    }
}
