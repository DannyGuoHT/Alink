package com.alibaba.alink.batchoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;
import org.apache.flink.table.api.TableSchema;


public class DBSinkBatchOp extends AlinkSinkBatchOp {

    private AlinkDB db;
    private String tableName;
    private TableSchema schema = null;

    public DBSinkBatchOp(AlinkDB db, String tableName) {
        this(db, tableName, null);
    }

    public DBSinkBatchOp(AlinkDB db, String tableName, AlinkParameter parameter) {
        this(db,
                new AlinkParameter().put(parameter)
                        .put(AnnotationUtils.annotationAlias(db.getClass()), tableName)
        );
    }

    public DBSinkBatchOp(AlinkDB db, AlinkParameter parameter) {
        super(AnnotationUtils.annotationName(db.getClass()), db.getParams().put(parameter));

        this.db = db;
        this.tableName = parameter.getString(AnnotationUtils.annotationAlias(db.getClass()));
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        //Get table schema
        this.schema = in.getSchema();

        //Create Table
        try {
            if (this.isOverwriteSink()) {
                if (db.hasTable(this.tableName)) {
                    db.dropTable(this.tableName);
                }
            }

            db.createTable(this.tableName, this.schema, this.params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //Sink to DB
        db.sinkBatch(this.tableName, in.getTable(), this.params);

        return this;
    }
}
