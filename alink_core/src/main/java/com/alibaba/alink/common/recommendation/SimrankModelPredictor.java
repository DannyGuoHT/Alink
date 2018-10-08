package com.alibaba.alink.common.recommendation;

import java.util.List;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.ArrayUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

public class SimrankModelPredictor extends AlinkPredictor {

    private SimrankModel model = null;
    private int itemColIdx = -1;
    private KeepColNamesManager keepColManager = null;

    public SimrankModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        String predResultColName = this.params.getString("predResultColName");
        this.keepColManager = new KeepColNamesManager(modelScheme, dataSchema, new String[] {predResultColName},
            params);
    }

    @Override
    public void loadModel(List <Row> modelRows) {
        this.model = new SimrankModel();
        model.load(modelRows);

        String userColName = model.getMeta().getString("userColName");
        String itemColName = model.getMeta().getString("itemColName");

        for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
            if (itemColName.equals(dataSchema.getColumnName(i).get())) { itemColIdx = i; }
        }

        if (itemColIdx == -1) { throw new RuntimeException("can't find column " + itemColName + " in the test data"); }
    }

    @Override
    public TableSchema getResultSchema() {
        String predResultColName = this.params.getString("predResultColName");
        return new TableSchema(
            ArrayUtil.arrayMerge(keepColManager.getKeepColNames(), predResultColName),
            ArrayUtil.arrayMerge(keepColManager.getKeepColTypes(), Types.STRING())
        );
    }

    @Override
    public Row predict(Row row) throws Exception {
        return this.model.predict(row, itemColIdx, keepColManager.getKeepColIndices());
    }
}