package com.alibaba.alink.common.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;

/**
 * *
 * The implementation of Predictor used by Box Plot Model Batch operator
 *
 */
public class BoxPlotModelPredictor extends AlinkPredictor {
    private BoxPlotModel model = null;
    HashMap<String, HashMap<String, Double>> data = new HashMap<String, HashMap<String, Double>>();

    private int n;
    private int[] idx;
    private String[] featureNames = null;

    /**
     * Constructor for BoxPlotModelPredictor
     *
     * @param modelScheme model schema
     * @param dataSchema  input data schema
     * @param params      ALink parameter
     */
    public BoxPlotModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);

    }

    /**
     * Constructor for BoxPlotModelPredictor
     *
     * @param model                box plot model
     * @param predictTableColNames predict table col names
     */
    public BoxPlotModelPredictor(BoxPlotModel model, String[] predictTableColNames) {
        super(null, null, null);
        this.model = model;
        data = model.getData();
        String[] featureColNames = model.getMeta().getStringArray(ParamName.featureColNames);
        n = featureColNames.length;
        this.idx = new int[this.n];
        for (int i = 0; i < n; i++) {
            idx[i] = TableUtil.findIndexFromName(predictTableColNames, featureColNames[i]);
        }
    }

    /**
     * load box plot model rows
     *
     * @param modelRows Box Plot model rows
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        model = new BoxPlotModel();
        model.load(modelRows);
        data = model.getData();
        String[] dataColNames = dataSchema.getColumnNames();
        featureNames = this.model.getMeta().getStringArray(ParamName.featureColNames);
        n = featureNames.length;
        this.idx = new int[this.n];
        for (int i = 0; i < n; i++) {
            idx[i] = TableUtil.findIndexFromName(dataColNames, featureNames[i]);
        }

    }

    /**
     * Get result table schema
     *
     * @return table schema
     */
    @Override
    public TableSchema getResultSchema() {
        String predResultColName = this.params.getString("predResultColName");
        return new TableSchema(
                ArrayUtil.arrayMerge(dataSchema.getColumnNames(), predResultColName),
                ArrayUtil.arrayMerge(dataSchema.getTypes(), Types.DOUBLE())
        );
    }

    /**
     * implement predict interface
     *
     * @param row input row data
     * @return row with predict result
     */
    @Override
    public Row predict(Row row) throws Exception {
        Row r = new Row(row.getArity() + 1);
        for (int i = 0; i < row.getArity(); i++) {
            r.setField(i, row.getField(i));
        }

        Double anomlyScore = 0.0;
        //get parameter from params
        Double k = params.getDoubleOrDefault("K", 1.5);
        int anomlyFeatureNumber = params.getIntegerOrDefault("anomlyFeatureNumber", 1);
        int m = 0;
        //Count anomly score
        for (int i = 0; i < this.idx.length; i++) {
            double q3 = data.get(featureNames[i]).get("q3");
            double q1 = data.get(featureNames[i]).get("q1");
            double range = q3 - q1;
            double value = Double.parseDouble(row.getField(idx[i]).toString());
            if ((value < q1 && ((q1 - value) / range) > k) || (value > q3 && (value - q3) / range > k)) {
                m++;
                if (m == anomlyFeatureNumber) {
                    anomlyScore = 1.0;
                    break;
                }
            }

        }
        r.setField(row.getArity(), anomlyScore);
        return r;
    }

}
