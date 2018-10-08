package com.alibaba.alink.streamoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.AlinkIOType;
import com.alibaba.alink.io.AlinkIOTypeAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.streamoperator.StreamOperator;

@AlinkIOTypeAnnotation(type = AlinkIOType.SinkStream)
public abstract class AlinkSinkStreamOp extends StreamOperator {
    protected AlinkSinkStreamOp(String alinkSrcSnkName, AlinkParameter params) {
        super(params);
        this.params.put(ParamName.alinkIOType, AlinkIOType.SinkStream, AlinkIOType.class)
                .put(ParamName.alinkIOName, alinkSrcSnkName);
    }

    public static AlinkSinkStreamOp of(AlinkParameter params) throws Exception {
        if (params.contains(ParamName.alinkIOType)
                && params.get(ParamName.alinkIOType, AlinkIOType.class).equals(AlinkIOType.SinkStream)
                && params.contains(ParamName.alinkIOName)) {
            if (AlinkDB.isAlinkDB(params)) {
                return new DBSinkStreamOp(AlinkDB.of(params), params);
            } else if (params.contains(ParamName.alinkIOName)) {
                String name = params.getString(ParamName.alinkIOName);
                return (AlinkSinkStreamOp) AnnotationUtils.createOp(name, AlinkIOType.SinkStream, params);
            }
        }
        throw new RuntimeException("Parameter Error.");

    }
}
