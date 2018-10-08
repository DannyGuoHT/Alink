package com.alibaba.alink.common.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.ArrayUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;

public class ALSTopKModelPredictor extends AlinkPredictor {

    private ALSTopKModel model = null;
    private int userColIdx = -1;
    private KeepColNamesManager keepColManager = null;

    public ALSTopKModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        String predResultColName = this.params.getString("predResultColName");
        this.keepColManager = new KeepColNamesManager(modelScheme, dataSchema, new String[]{predResultColName}, params);
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        this.model = new ALSTopKModel();
        model.load(modelRows);

        String userColName = model.getMeta().getString("userColName");
        String itemColName = model.getMeta().getString("itemColName");

        for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
            if(userColName.equals(dataSchema.getColumnName(i).get()))
                userColIdx = i;
        }

        if(userColIdx == -1)
            throw new RuntimeException("can't find column " + userColName + " in the test data");
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
        return this.model.predict(row, userColIdx, keepColManager.getKeepColIndices());
    }
}
