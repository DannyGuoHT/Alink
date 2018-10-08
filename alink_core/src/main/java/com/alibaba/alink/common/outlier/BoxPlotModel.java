package com.alibaba.alink.common.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.types.Row;

import java.util.*;

import static com.alibaba.alink.common.AlinkSession.gson;

/**
 * The implementation of Box Plot Model used in Box Plot batch operator
 *
 */
public class BoxPlotModel extends AlinkModel {

    HashMap<String, HashMap<String, Double>> data = null;

    /**
     * Default Constructor for BoxPlotModel
     */
    public BoxPlotModel() {
    }

    /**
     * Constructor for BoxPlotModel
     *
     * @param modelName       model name
     * @param featureColNames
     */
    public BoxPlotModel(String modelName, String[] featureColNames, HashMap<String, HashMap<String, Double>> data) {
        this.meta.put(ParamName.featureColNames, featureColNames)
                .put(ParamName.modelName, modelName);

        this.data = data;
    }

    /**
     * get Meta of this Model
     */
    public AlinkParameter getMeta() {
        return this.meta;
    }

    /**
     * get Model data
     */
    public HashMap<String, HashMap<String, Double>> getData() {
        return data;
    }

    /**
     * load meta and data from a list of rows
     *
     * @param rows model content rows
     */
    @Override
    public void load(List<Row> rows) {
        // must get the meta first
        this.meta = AlinkModel.getMetaParams(rows);
        data = new HashMap<String, HashMap<String, Double>>();

        if (this.meta == null) {
            throw new RuntimeException("fail to load Box Plot model meta");
        }
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            }
            String featureName = row.getField(1).toString();
            String json_string = row.getField(2).toString();
            HashMap<String, Double> maps = gson.fromJson(json_string, HashMap.class);
            String[] featureNames = meta.getStringArray(ParamName.featureColNames);
            if (TableUtil.findIndexFromName(featureNames, featureName) >= 0)
                data.put(featureName, maps);
            else
                throw new RuntimeException("feature name doesn't correspond to meta");

        }
    }

    /**
     * save meta and data to a list of rows
     * @return model list
     */
    @Override
    public List<Row> save() {
        ArrayList<Row> list = new ArrayList<>();
        long id = 0L;
        list.add(Row.of(new Object[]{id++, this.meta.toJson(), "null", "null"}));
        Iterator<Map.Entry<String, HashMap<String, Double>>> entries = data.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, HashMap<String, Double>> entry = entries.next();
            list.add(Row.of(new Object[]{id++, entry.getKey(), gson.toJson(entry.getValue(), HashMap.class), "null"}));

        }
        return list;
    }
}
