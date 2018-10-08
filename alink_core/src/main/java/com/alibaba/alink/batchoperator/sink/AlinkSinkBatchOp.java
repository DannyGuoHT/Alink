package com.alibaba.alink.batchoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.AlinkIOType;
import com.alibaba.alink.io.AlinkIOTypeAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;

@AlinkIOTypeAnnotation(type = AlinkIOType.SinkBatch)
public abstract class AlinkSinkBatchOp extends BatchOperator {

    protected AlinkSinkBatchOp(String alinkSrcSnkName, AlinkParameter params) {
        super(params);
        this.params.put(ParamName.alinkIOType, AnnotationUtils.annotationType(this.getClass()), AlinkIOType.class)
                .put(ParamName.alinkIOName, alinkSrcSnkName);
    }

    public boolean isOverwriteSink() {
        return this.params.getBoolOrDefault(ParamName.overwriteSink, false);
    }

    public static AlinkSinkBatchOp of(AlinkParameter params) throws Exception {
        if (params.contains(ParamName.alinkIOType)
                && params.get(ParamName.alinkIOType, AlinkIOType.class).equals(AlinkIOType.SinkBatch)
                && params.contains(ParamName.alinkIOName)) {
            if (AlinkDB.isAlinkDB(params)) {
                return new DBSinkBatchOp(AlinkDB.of(params), params);
            } else if (params.contains(ParamName.alinkIOName)) {
                String name = params.getString(ParamName.alinkIOName);
                return (AlinkSinkBatchOp) AnnotationUtils.createOp(name, AlinkIOType.SinkBatch, params);
            }
        }
        throw new RuntimeException("Parameter Error.");

    }
}
