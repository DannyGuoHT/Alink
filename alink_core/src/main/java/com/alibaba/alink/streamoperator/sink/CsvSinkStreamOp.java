package com.alibaba.alink.streamoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.io.AlinkIONameAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.CsvTableSink;


@AlinkIONameAnnotation(name = "csv")
public class CsvSinkStreamOp extends AlinkSinkStreamOp {

    private TableSchema schema;

    public CsvSinkStreamOp(String filePath) {
        this(new AlinkParameter().put("filePath", filePath));
    }

    public CsvSinkStreamOp(String filePath, String fieldDelimiter) {
        this(new AlinkParameter().put("filePath", filePath).putIgnoreNull("fieldDelimiter", fieldDelimiter));
    }

    public CsvSinkStreamOp(AlinkParameter params) {
        super(AnnotationUtils.annotationName(CsvSinkStreamOp.class), params);
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        this.schema = in.getSchema();

        String filePath = this.params.getString("filePath");
        String fieldDelimiter = this.params.getStringOrDefault("fieldDelimiter", ",");

        int numFiles = this.params.getIntegerOrDefault("numFiles", 1);
        CsvTableSink cts = new CsvTableSink(filePath, fieldDelimiter, numFiles, FileSystem.WriteMode.NO_OVERWRITE);

        in.getTable().writeToSink(cts);
        return this;
    }
}
