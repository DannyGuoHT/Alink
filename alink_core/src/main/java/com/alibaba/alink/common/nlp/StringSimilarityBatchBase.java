package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Create the StringsimilarityBase class which calculates the similarity, set the output data.
 */

public class StringSimilarityBatchBase implements Serializable {
    private Boolean text;
    private String[] keepColNames;
    private AlinkParameter params;

    public StringSimilarityBatchBase(AlinkParameter params, Boolean text){
        this.keepColNames = params.getStringArrayOrDefault(ParamName.keepColNames, StringSimilarityConst.KEEPCOLNAMES);
        this.text = text;
        this.params = params;
    }

    public DataSet<Row> stringSimilarityRes(DataSet<Row> data) {
        // Create the StringSimilarityBase class the calcute the similarity between two characters.
        final StringSimilarityBase base = new StringSimilarityBase(params, text);

        // Calculate the similarity in pair, and set the output as Row.
        DataSet<Row> res = data.map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row row) throws Exception {
                    Double d = base.stringSimilairtyRes(row);
                    Row out = new Row(1 + keepColNames.length);
                    out.setField(0, d);
                    for(int i = 0; i < keepColNames.length; i++){
                        out.setField(i + 1, row.getField(i + 2));
                    }
                    return out;
                }
            });

        return res;
    }
}
