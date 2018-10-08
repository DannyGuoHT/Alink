package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.nlp.segment.SegToken;
import com.alibaba.alink.common.nlp.segment.WordSegmenter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class SegmentUDF extends ScalarFunction {

    private final WordSegmenter segmentor;
    private final String wordDelimeter;

    public SegmentUDF(String wordDelimeter) {
        this.wordDelimeter = wordDelimeter;
        segmentor = new WordSegmenter();
    }

    public String eval(String content) {
        List <SegToken> tokens = segmentor.process(content, WordSegmenter.SegMode.SEARCH);
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            sbd.append(tokens.get(i).word);
            if (i < tokens.size() - 1)
                sbd.append(this.wordDelimeter);
        }
        return sbd.toString();
    }

    @Override
    public TypeInformation <?> getResultType(Class <?>[] signature) {
        return Types.STRING;
    }
}
