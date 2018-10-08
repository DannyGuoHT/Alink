package com.alibaba.alink.batchoperator.source;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.AlinkIOType;
import com.alibaba.alink.io.AlinkIOTypeAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;

@AlinkIOTypeAnnotation(type = AlinkIOType.SourceBatch)
public abstract class AlinkSourceBatchOp extends BatchOperator {

    protected AlinkSourceBatchOp(String alinkSrcSnkName, AlinkParameter params) {
        super(params);
        this.params.put(ParamName.alinkIOType, AlinkIOType.SourceBatch, AlinkIOType.class)
                .put(ParamName.alinkIOName, alinkSrcSnkName);

    }

    public static AlinkSourceBatchOp of(AlinkParameter params) throws Exception {
        if (params.contains(ParamName.alinkIOType)
                && params.get(ParamName.alinkIOType, AlinkIOType.class).equals(AlinkIOType.SourceBatch)
                && params.contains(ParamName.alinkIOName)) {
            if (AlinkDB.isAlinkDB(params)) {
                return new DBSourceBatchOp(AlinkDB.of(params), params);
            } else if (params.contains(ParamName.alinkIOName)) {
                String name = params.getString(ParamName.alinkIOName);
                return (AlinkSourceBatchOp) AnnotationUtils.createOp(name, AlinkIOType.SourceBatch, params);
            }
        }
        throw new RuntimeException("Parameter Error.");
    }
}
