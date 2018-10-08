package com.alibaba.alink.common.ml.feature;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.ml.MLModel;
import com.alibaba.alink.common.utils.AlinkModel;

import org.apache.flink.types.Row;

import static com.alibaba.alink.common.AlinkSession.gson;

public class OneHotModel extends MLModel {

    AlinkParameter meta = null;
    ArrayList <String> data = null;

    public OneHotModel() {
    }

    public OneHotModel(AlinkParameter meta, ArrayList <String> data) {
        this.meta = meta;
        this.data = data;
    }

    public ArrayList <String> getData() {
        return data;
    }

    @Override
    public void load(List <Row> rows) { // load meta and data from a list of rows
        this.meta = AlinkModel.getMetaParams(rows);
        String json = AlinkModel.extractStringFromModel(rows);
        this.data = gson.fromJson(json, ArrayList.class);
    }

    @Override
    public List <Row> save() { // save meta and data to a list of rows
        ArrayList <Row> list = new ArrayList <>();
        list.add(Row.of(0L, this.meta.toJson(), "", ""));
        AlinkModel.appendStringToModel(gson.toJson(this.data), list);
        return list;
    }
}
