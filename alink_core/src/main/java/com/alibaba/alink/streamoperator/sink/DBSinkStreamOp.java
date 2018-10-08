package com.alibaba.alink.streamoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.table.api.TableSchema;


public class DBSinkStreamOp extends AlinkSinkStreamOp {

    private AlinkDB db;
    private String tableName;
    private TableSchema schema;

    public DBSinkStreamOp(AlinkDB db, String tableName) {
        this(db, tableName, null);
    }

    public DBSinkStreamOp(AlinkDB db, String tableName, AlinkParameter parameter) {
        this(db,
                new AlinkParameter().put(parameter)
                        .put(AnnotationUtils.annotationAlias(db.getClass()), tableName)
        );
    }

    public DBSinkStreamOp(AlinkDB db, AlinkParameter parameter) {
        super(AnnotationUtils.annotationName(db.getClass()), db.getParams().put(parameter));

        this.db = db;
        this.tableName = parameter.getString(AnnotationUtils.annotationAlias(db.getClass()));
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        //Get table schema
        this.schema = in.getSchema();

        //Create Table
        try {
            if (!db.hasTable(this.tableName)) {
                db.createTable(this.tableName, this.schema, this.params);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

        //Sink to DB
        db.sinkStream(this.tableName, in.getTable(), this.params);

        return this;
    }
}
