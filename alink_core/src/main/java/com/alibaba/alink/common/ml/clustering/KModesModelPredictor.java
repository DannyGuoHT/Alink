package com.alibaba.alink.common.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.recommendation.KeepColNamesManager;
import com.alibaba.alink.common.constants.ClusterConstant;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;



public class KModesModelPredictor extends AlinkPredictor {

    private KModesModel model = null;
    private KeepColNamesManager keepColManager;
    private int[] colIdx;

    public KModesModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        String predResultColName = this.params.getStringOrDefault(ParamName.predResultColName, ClusterConstant.PRED_RESULT_COL_NAME);
        this.keepColManager = new KeepColNamesManager(modelScheme, dataSchema, new String[]{predResultColName}, params);
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        this.model = new KModesModel();
        model.load(modelRows);

        String[] featureColNames = model.getMeta().getStringArray(ParamName.featureColNames);
        colIdx = new int[featureColNames.length];

        for (int i = 0; i < featureColNames.length; i++) {
            colIdx[i] = TableUtil.findIndexFromName(dataSchema.getColumnNames(), featureColNames[i]);
            if(colIdx[i] < 0){
                throw new RuntimeException("can't find column in test data: " + featureColNames[i]);
            }
        }
    }

    @Override
    public TableSchema getResultSchema() {
        String predResultColName = this.params.getStringOrDefault(ParamName.predResultColName, ClusterConstant.PRED_RESULT_COL_NAME);
        return new TableSchema(
                ArrayUtil.arrayMerge(keepColManager.getKeepColNames(), predResultColName),
                ArrayUtil.arrayMerge(keepColManager.getKeepColTypes(), Types.LONG())
        );
    }

    @Override
    public Row predict(Row row) throws Exception {
        return this.model.predict(row, colIdx, keepColManager.getKeepColIndices());
    }
}
