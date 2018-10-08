package com.alibaba.alink.streamoperator.source;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.streamoperator.StreamOperator;


public class DBSourceStreamOp extends AlinkSourceStreamOp {

    public DBSourceStreamOp(AlinkDB db, String tableName) throws Exception {
        this(db, tableName, null);
    }

    public DBSourceStreamOp(AlinkDB db, String tableName, AlinkParameter parameter) throws Exception {
        this(db,
                new AlinkParameter().put(parameter)
                        .put(AnnotationUtils.annotationAlias(db.getClass()), tableName)
        );
    }

    public DBSourceStreamOp(AlinkDB db, AlinkParameter parameter) throws Exception {
        super(AnnotationUtils.annotationName(db.getClass()), db.getParams().put(parameter));

        String tableName = parameter.getString(AnnotationUtils.annotationAlias(db.getClass()));
        this.table = db.getStreamTable(tableName, this.params);
    }


    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


}
