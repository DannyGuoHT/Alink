package com.alibaba.alink.common.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.AlinkSession.gson;

public class ALSRatingModel extends AlinkModel {

    private float[][] userFactors = null;
    private float[][] itemFactors = null;
    private float[] userAvgFactors = null;
    private float[] itemAvgFactors = null;
    private Map<Long, Integer> userIdMap = null;
    private Map<Long, Integer> itemIdMap = null;

    public ALSRatingModel() {
    }

    /**
     * Load model meta and data from a trained model table.
     * This interface is called by the predictor.
     *
     * @param rows
     */
    @Override
    public void load(List<Row> rows) {
        // must get the meta first
        for (Row row : rows) {
            if ((long) row.getField(0) == 0) {
                // the first row stores the meta of the model
                this.meta = AlinkParameter.fromJson((String) row.getField(1));
                break;
            }
        }

        if (this.meta == null) {
            throw new RuntimeException("fail to load ALS model meta");
        }

        String task = this.meta.getString("task");

        if (!task.equalsIgnoreCase("rating"))
            throw new RuntimeException("the loaded model is not trained for rating, please retrain it and set task = \"rating\"");

        int numUsers = 0;
        int numItems = 0;
        userIdMap = new HashMap<>();
        itemIdMap = new HashMap<>();

        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                long nodeId = Long.valueOf((String) row.getField(2));
                byte who = Byte.valueOf((String) row.getField(1));
                if (who == 0) {
                    userIdMap.put(nodeId, numUsers);
                    numUsers++;
                } else {
                    itemIdMap.put(nodeId, numItems);
                    numItems++;
                }
            }
        }

        userFactors = new float[numUsers][0];
        itemFactors = new float[numItems][0];

        numUsers = 0;
        numItems = 0;
        int numFactors = 0;

        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                byte who = Byte.valueOf((String) row.getField(1));
                float[] factors = gson.fromJson((String) row.getField(3), float[].class);
                numFactors = factors.length;
                if (who == 0) {
                    userFactors[numUsers] = factors;
                    numUsers++;
                } else {
                    itemFactors[numItems] = factors;
                    numItems++;
                }
            }
        }

        userAvgFactors = new float[numFactors];
        itemAvgFactors = new float[numFactors];
        Arrays.fill(userAvgFactors, 0.F);
        Arrays.fill(itemAvgFactors, 0.F);

        for (int i = 0; i < numUsers; i++) {
            for (int j = 0; j < numFactors; j++) {
                userAvgFactors[j] += userFactors[i][j];
            }
        }

        for (int i = 0; i < numItems; i++) {
            for (int j = 0; j < numFactors; j++) {
                itemAvgFactors[j] += itemFactors[i][j];
            }
        }

        for (int i = 0; i < numFactors; i++) {
            userAvgFactors[i] /= numUsers;
            itemAvgFactors[i] /= numItems;
        }
    }

    /**
     * Save the model to a list of rows that conform to AlinkModel's standard format.
     * This is called by the train batch operator to save model to a table.
     *
     * @return
     */
    @Override
    public List<Row> save() { // save meta and data to a list of rows
        throw new RuntimeException("should not call this function.");
    }

    /**
     * Predict
     */

    public Row predict(Row row, int userColIdx, int itemColIdx, final int[] keepColIdx) throws Exception {
        int keepSize = keepColIdx.length;
        Row r = new Row(keepSize + 1);
        for (int i = 0; i < keepSize; i++) {
            r.setField(i, row.getField(keepColIdx[i]));
        }

        long userId = ((Number) row.getField(userColIdx)).longValue();
        long itemId = ((Number) row.getField(itemColIdx)).longValue();

        int numUsers = userFactors.length;
        int numItems = itemFactors.length;

        int userLocalId = userIdMap.getOrDefault(userId, numUsers);
        int itemLocalId = itemIdMap.getOrDefault(itemId, numItems);

        float[] predUserFactors = null;
        float[] predItemFactors = null;

        predUserFactors = userLocalId < numUsers ? userFactors[userLocalId] : userAvgFactors;
        predItemFactors = itemLocalId < numItems ? itemFactors[itemLocalId] : itemAvgFactors;

        double v = 0.;
        int n = predUserFactors.length;
        for (int i = 0; i < n; i++) {
            v += predUserFactors[i] * predItemFactors[i];
        }
        r.setField(keepSize, v);

        return r;
    }
}
