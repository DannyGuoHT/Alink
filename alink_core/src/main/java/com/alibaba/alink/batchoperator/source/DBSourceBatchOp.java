package com.alibaba.alink.batchoperator.source;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;


public class DBSourceBatchOp extends AlinkSourceBatchOp {

    public DBSourceBatchOp(AlinkDB db, String tableName) throws Exception {
        this(db, tableName, null);
    }

    public DBSourceBatchOp(AlinkDB db, String tableName, AlinkParameter parameter) throws Exception {
        this(db,
                new AlinkParameter().put(parameter)
                        .put(AnnotationUtils.annotationAlias(db.getClass()), tableName)
        );
    }

    public DBSourceBatchOp(AlinkDB db, AlinkParameter parameter) throws Exception {
        super(AnnotationUtils.annotationName(db.getClass()), db.getParams().put(parameter));

        String tableName = parameter.getString(AnnotationUtils.annotationAlias(db.getClass()));
        this.table = db.getBatchTable(tableName, this.params);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new UnsupportedOperationException("Not supported.");
    }


}
