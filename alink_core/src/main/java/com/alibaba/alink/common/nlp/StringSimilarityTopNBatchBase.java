package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StringSimilarityTopNBatchBase implements Serializable {
    private Integer topN;
    private Boolean text;
    private String[] leftKeepColNames;
    private String[] rightKeepColNames;
    private AlinkParameter params;

    public StringSimilarityTopNBatchBase(AlinkParameter params, Boolean text){
        this.topN = params.getIntegerOrDefault("topN", StringSimilarityConst.TOP_N);
        this.text = text;
        this.leftKeepColNames = params.getStringArrayOrDefault("leftKeepColNames", StringSimilarityConst.KEEPCOLNAMES);
        this.rightKeepColNames = params.getStringArrayOrDefault("rightKeepColNames", StringSimilarityConst.KEEPCOLNAMES);
        this.params = params;
    }

    public DataSet<Row> stringSimilarityRes(DataSet<Row> left, DataSet<Row> right){
        final StringSimilarityBase base = new StringSimilarityBase(params, text);
        DataSet<Row> out = left
                .mapPartition(new CalSimilarityRichMapPartitionFunc(base, topN))
                .withBroadcastSet(right, "right")
                .flatMap(new FlatMapFunction<ArrayList<ArrayList<Row>>, Row>() {
                    @Override
                    public void flatMap(ArrayList<ArrayList<Row>> rows, Collector<Row> collector) throws Exception {
                        for(int i = 0; i < rows.size(); i++){
                            for(Row row : rows.get(i)){
                                collector.collect(row);
                            }
                        }
                    }
                });

        return out;
    }

    class CalSimilarityRichMapPartitionFunc extends RichMapPartitionFunction<Row, ArrayList<ArrayList<Row>>> {
        private List<Row> right;
        private StringSimilarityBase base;
        private int topN;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.right = getRuntimeContext().getBroadcastVariable("right");
        }

        public CalSimilarityRichMapPartitionFunc(StringSimilarityBase base, int topN){
            this.base = base;
            this.topN = topN;
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<ArrayList<ArrayList<Row>>> collector) throws Exception {
            ArrayList<ArrayList<Row>> record = new ArrayList<>();
            for(Row row : iterable){
                ArrayList<Row> cur = new ArrayList<>();
                for(int i = 0; i < right.size(); i++){
                    Row tmp = new Row(2);
                    tmp.setField(0, row.getField(0));
                    tmp.setField(1, right.get(i).getField(0));
                    Double d = base.stringSimilairtyRes(tmp);
                    Row out = new Row(1 + leftKeepColNames.length + rightKeepColNames.length);
                    out.setField(0, d);
                    for(int j = 0; j < leftKeepColNames.length; j++){
                        out.setField(j + 1, row.getField(j + 1));
                    }
                    for(int j = 0; j < rightKeepColNames.length; j++){
                        out.setField(1 + leftKeepColNames.length + j, right.get(i).getField(j + 1));
                    }
                    cur.add(out);
                }
                Collections.sort(cur, new Comparator<Row>() {
                    @Override
                    public int compare(Row r1, Row r2) {
                        Double d1 = (Double) r1.getField(0);
                        Double d2 = (Double) r2.getField(0);
                        return d1.compareTo(d2);
                    }
                });
                ArrayList<Row> out = new ArrayList<>();
                for (int j = cur.size() - 1; j >= Math.max(0, cur.size() - topN); j--) {
                    out.add(cur.get(j));
                }
                record.add(out);
            }
            collector.collect(record);
        }
    }

}
