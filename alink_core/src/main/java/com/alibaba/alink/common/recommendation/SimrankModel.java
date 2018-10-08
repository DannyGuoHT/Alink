package com.alibaba.alink.common.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.*;

public class SimrankModel extends AlinkModel {

    private String[] itemSim = null;
    private Iterator<Tuple2<Integer, String>> data = null;

    private Map<Long, Integer> userIdMap = null;
    private Map<Long, Integer> itemIdMap = null;


    public SimrankModel() {
    }

    public SimrankModel(Iterator<Tuple2<Integer, String>> data) {
        this.data = data;
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
            throw new RuntimeException("fail to load Simrank model meta");
        }

        int numUsers = super.meta.getInteger("numUsers");
        int numItems = super.meta.getInteger("numItems");

        itemSim = new String[numItems];
        userIdMap = new HashMap<>();
        itemIdMap = new HashMap<>();

        int userCount = 0;
        int itemCount = 0;

        // get the model data
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                long index = Long.valueOf((String) row.getField(1));
                String topk = ((String) row.getField(2));
                itemIdMap.put(index, itemCount);
                itemSim[itemCount] = topk;
                itemCount++;
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

        int numUsers = 0;
        int numItems = 0;

        while (this.data.hasNext()) {
            Tuple2<Integer, String> r = this.data.next();
            numItems++;

            // input row: Tuple2(itemid, topk)
            list.add(Row.of(new Object[]{id++, String.valueOf(r.getField(0)), String.valueOf(r.getField(1)), null}));
        }

        super.meta.put("numUsers", numUsers);
        super.meta.put("numItems", numItems);

        // the first row: model meta
        list.add(Row.of(new Object[]{0L, super.meta.toJson(), "null", "null"}));

        return list;
    }

    /**
     * Predict
     */
    public Row predict(Row row, int itemColIdx, final int[] keepColIdx) throws Exception {
        int keepSize = keepColIdx.length;
        Row r = new Row(keepSize + 1);
        for (int i = 0; i < keepSize; i++) {
            r.setField(i, row.getField(keepColIdx[i]));
        }

        long itemId = ((Number) row.getField(itemColIdx)).longValue();
        int itemLocalId = itemIdMap.getOrDefault(itemId, itemSim.length);

        if (itemLocalId >= itemSim.length) {
            r.setField(keepSize, null);
        } else {
            r.setField(keepSize, itemSim[itemLocalId]);
        }
        return r;
    }
}
