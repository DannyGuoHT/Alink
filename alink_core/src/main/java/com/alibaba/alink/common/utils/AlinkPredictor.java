package com.alibaba.alink.common.utils;

import java.util.List;

import com.alibaba.alink.common.AlinkParameter;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public abstract class AlinkPredictor {

    public static String CLASS_CANONICAL_NAME = "AlinkPredictorClassCanonicalName";

    protected TableSchema modelScheme;
    protected TableSchema dataSchema;
    protected AlinkParameter params;

    public AlinkPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        this.modelScheme = modelScheme;
        this.dataSchema = dataSchema;
        this.params = params;
    }

    public abstract void loadModel(List <Row> modelRows);

    public abstract TableSchema getResultSchema();

    public abstract Row predict(Row row) throws Exception;
}
