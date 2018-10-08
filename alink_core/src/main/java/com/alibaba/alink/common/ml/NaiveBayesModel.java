package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;

import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.AlinkSession.gson;

/**
 * *
 * the model of naive bayes.
 *
 */
public class NaiveBayesModel extends MLModel {

    private AlinkParameter meta = null;
    private NaiveBayesProbInfo data = null;

    public NaiveBayesModel() {
    }

    public NaiveBayesModel(AlinkParameter meta, NaiveBayesProbInfo data) {
        this.meta = meta;
        this.data = data;
    }

    public AlinkParameter getMetaInfo() {
        return this.meta;
    }

    public NaiveBayesProbInfo getData() {
        return data;
    }

    /**
     * // load meta and data from a list of rows
     * @param rows model rows.
     */
    @Override
    public void load(List<Row> rows) {
        this.meta = AlinkModel.getMetaParams(rows);
        String json = AlinkModel.extractStringFromModel(rows);
        this.data = gson.fromJson(json, NaiveBayesProbInfo.class);

    }

    /**
     * save meta and data to a list of rows.
     */
    @Override
    public List<Row> save() {
        ArrayList<Row> list = new ArrayList<>();
        list.add(Row.of(0L, this.meta.toJson(), "", ""));
        AlinkModel.appendStringToModel(gson.toJson(this.data), list);
        return list;
    }
}
