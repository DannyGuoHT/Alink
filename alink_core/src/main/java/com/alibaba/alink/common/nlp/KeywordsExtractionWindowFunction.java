package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Deal with the document within the timeInterval.
 */
public class KeywordsExtractionWindowFunction implements AllWindowFunction<Row, Row, TimeWindow> {
    private AlinkParameter params;

    public KeywordsExtractionWindowFunction(AlinkParameter params) {
        this.params = params;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Row> iterable, Collector<Row> collector) throws Exception {
        TextRank textRank = new TextRank(params);
        for(Row row : iterable) {
            Row[] out = textRank.getKeyWords(row);
            for (int i = 0; i < out.length; i++) {
                collector.collect(out[i]);
            }
        }
    }
}
