package com.alibaba.alink.batchoperator.outlier;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class BoxPlotGroupReduceFunction implements GroupReduceFunction<Row, Row> {

    private String featureName;
    private int featureIndex;

    public BoxPlotGroupReduceFunction(String featureName) {
        this.featureName = featureName;
    }

    @Override
    public void reduce(Iterable<Row> values, Collector<Row> out) {
        ArrayList<Double> doubleList = new ArrayList<Double>();
        HashMap<String, Double> modelMap = new HashMap<String, Double>();
        for (Row value : values) {
            double v = Double.parseDouble(value.getField(0).toString());
            doubleList.add(v);
        }
        Collections.sort(doubleList);
        if (doubleList.size() != 5)
            throw new RuntimeException("Percentile error");
        double min = doubleList.get(0);
        modelMap.put("min", min);
        double max = doubleList.get(4);
        modelMap.put("max", max);
        double q1 = doubleList.get(1);
        modelMap.put("q1", q1);
        double mid = doubleList.get(2);
        modelMap.put("mid", mid);
        double q3 = doubleList.get(3);
        modelMap.put("q3", q3);
        JSONObject jsonObject = JSONObject.fromObject(modelMap);
        out.collect(Row.of(new Object[]{featureName, jsonObject.toString()}));

    }

}



