package com.alibaba.alink.common.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import org.apache.flink.types.Row;

import java.util.*;

public class ALSTopKModel extends AlinkModel {

    private String[] userFavorite = null;
    private String userAvgFavorites = null;
    private Iterator<Row> data = null;

    private Map<Long, Integer> userIdMap = null;
    private int taskId;


    public ALSTopKModel() {
    }

    public ALSTopKModel(int taskId, Iterator<Row> data) {
        this.data = data;
        this.taskId = taskId;
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

        int numUsers = rows.size() - 1;
        String task = this.meta.getString("task");

        if (!task.equalsIgnoreCase("topK"))
            throw new RuntimeException("the loaded model is not trained for topK prediction, please retrain it and set task = \"topK\"");

        userFavorite = new String[numUsers];
        userIdMap = new HashMap<>();

        int userCount = 0;

        // get the model data
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                String userOrItem = ((String) row.getField(1));
                long index = Long.valueOf((String) row.getField(2));
                String favorites = ((String) row.getField(3));
                if (userOrItem.equals("user")) {
                    userIdMap.put(index, userCount);
                    userFavorite[userCount] = favorites;
                    userCount++;
                }  else {
                    throw new RuntimeException("unexpected");
                }
            }
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
        ArrayList<Row> list = new ArrayList<>();
        long id = 1L;

        while (this.data.hasNext()) {
            Row r = this.data.next();
            // input row: Tuple3(userOrItem, id, favorites)
            list.add(Row.of(new Object[]{id++, r.getField(0), String.valueOf(r.getField(1)), r.getField(2)}));
        }

        if(taskId == 0) {
            // the first row: model meta
            list.add(Row.of(new Object[]{0L, super.meta.toJson(), "null", "null"}));
        }

        return list;
    }

    /**
     * Predict
     */
    public Row predict(Row row, int userColIdx, final int[] keepColIdx) throws Exception {
        int keepSize = keepColIdx.length;
        Row r = new Row(keepSize + 1);
        for (int i = 0; i < keepSize; i++) {
            r.setField(i, row.getField(keepColIdx[i]));
        }

        long userId = ((Number) row.getField(userColIdx)).longValue();
        int userLocalId = userIdMap.getOrDefault(userId, userFavorite.length);

        if (userLocalId >= userFavorite.length) {
            r.setField(keepSize, userAvgFavorites);
        } else {
            r.setField(keepSize, userFavorite[userLocalId]);
        }
        return r;
    }
}
