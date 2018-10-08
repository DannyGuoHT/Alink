package com.alibaba.alink.common.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.recommendation.KeepColNamesManager;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;

public class SOSModelPredictor extends AlinkPredictor {
    private SOSModel model = null;
    private String predResultColName = null;
    private int[] selectedColIndices = null;
    private KeepColNamesManager keepColManager = null;


    public SOSModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        this.predResultColName = params.getString(ParamName.predResultColName);
        this.keepColManager = new KeepColNamesManager(modelScheme, dataSchema, new String[]{predResultColName}, params);
        this.model = new SOSModel();
    }

    @Override
    public void loadModel(List <Row> modelRows) {
        this.model.load(modelRows);

        String[] featureColNames = this.model.getMeta().getStringArray("featureColNames");
        this.selectedColIndices = new int[featureColNames.length];
        for (int i = 0; i < featureColNames.length; i++) {
            this.selectedColIndices[i] = TableUtil.findIndexFromName(super.dataSchema.getColumnNames(), featureColNames[i]);
        }
    }

    @Override
    public TableSchema getResultSchema() {
        String predResultColName = this.params.getString("predResultColName");
        return new TableSchema(
                ArrayUtil.arrayMerge(keepColManager.getKeepColNames(), predResultColName),
                ArrayUtil.arrayMerge(keepColManager.getKeepColTypes(), Types.DOUBLE())
        );
    }

    @Override
    public Row predict(Row row) throws Exception {
        return this.model.predict(row, this.selectedColIndices, keepColManager.getKeepColIndices());
    }
}
