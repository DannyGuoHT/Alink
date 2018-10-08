package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Deal with the data within the timeInterval
 * Create the StringsimilarityBase class which calculates the similarity, set the output data.
 */

public class StringSimilarityWindowFunction implements AllWindowFunction<Row, Row, TimeWindow> {
    private Boolean text;
    private String[] keepColNames;
    private AlinkParameter params;

    public StringSimilarityWindowFunction(AlinkParameter params, Boolean text){
        this.params = params;
        this.text = text;
        this.keepColNames = params.getStringArrayOrDefault(ParamName.keepColNames, StringSimilarityConst.KEEPCOLNAMES);
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Row> iterable, Collector<Row> collector) throws Exception {
        // Create the StringSimilarityBase class the calcute the similarity between two characters.
        StringSimilarityBase base = new StringSimilarityBase(params, text);
        ArrayList<Row> record = new ArrayList<>();

        // Calculate the similarity in pair, and set the output as Row.
        for(Row row : iterable){
            Double d = base.stringSimilairtyRes(row);
            Row out = new Row(1 + keepColNames.length);
            out.setField(0, d);
            for(int i = 0; i < keepColNames.length; i++){
                out.setField(i + 1, row.getField(i + 2));
            }
            record.add(out);
        }
        for(int i = 0; i < record.size(); i++){
            collector.collect(record.get(i));
        }
    }
}
