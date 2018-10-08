package com.alibaba.alink.streamoperator.source;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.AlinkIOType;
import com.alibaba.alink.io.AlinkIOTypeAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.streamoperator.StreamOperator;

@AlinkIOTypeAnnotation(type = AlinkIOType.SourceStream)
public abstract class AlinkSourceStreamOp extends StreamOperator {
    protected AlinkSourceStreamOp(String alinkSrcSnkName, AlinkParameter params) {
        super(params);
        this.params.put(ParamName.alinkIOType, AlinkIOType.SourceStream, AlinkIOType.class)
                .put(ParamName.alinkIOName, alinkSrcSnkName);
    }

    public static AlinkSourceStreamOp of(AlinkParameter params) throws Exception {
        if (params.contains(ParamName.alinkIOType)
                && params.get(ParamName.alinkIOType, AlinkIOType.class).equals(AlinkIOType.SourceStream)
                && params.contains(ParamName.alinkIOName)) {
            if (AlinkDB.isAlinkDB(params)) {
                return new DBSourceStreamOp(AlinkDB.of(params), params);
            } else if (params.contains(ParamName.alinkIOName)) {
                String name = params.getString(ParamName.alinkIOName);
                return (AlinkSourceStreamOp) AnnotationUtils.createOp(name, AlinkIOType.SourceStream, params);
            }
        }
        throw new RuntimeException("Parameter Error.");
    }
}
