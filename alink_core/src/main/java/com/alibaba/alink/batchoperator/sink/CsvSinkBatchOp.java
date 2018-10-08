package com.alibaba.alink.batchoperator.sink;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.AlinkIONameAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.CsvTableSink;


@AlinkIONameAnnotation(name = "csv")
public class CsvSinkBatchOp extends AlinkSinkBatchOp {

    private TableSchema schema;

    public CsvSinkBatchOp(String filePath) {
        this(new AlinkParameter().put("filePath", filePath));
    }

    public CsvSinkBatchOp(String filePath, String fieldDelimiter) {
        this(new AlinkParameter().put("filePath", filePath).putIgnoreNull("fieldDelimiter", fieldDelimiter));
    }

    public CsvSinkBatchOp(String filePath, String fieldDelimiter, boolean overwriteSink) {
        this(new AlinkParameter()
                .put("filePath", filePath)
                .putIgnoreNull("fieldDelimiter", fieldDelimiter)
                .put(ParamName.overwriteSink, overwriteSink)
        );
    }

    public CsvSinkBatchOp(AlinkParameter params) {
        super(AnnotationUtils.annotationName(CsvSinkBatchOp.class), params);
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        this.schema = in.getSchema();

        String filePath = this.params.getString("filePath");
        String fieldDelimiter = this.params.getStringOrDefault("fieldDelimiter", ",");

        int numFiles = this.params.getIntegerOrDefault("numFiles", 1);

        FileSystem.WriteMode mode = FileSystem.WriteMode.NO_OVERWRITE;
        if (this.isOverwriteSink()) {
            mode = FileSystem.WriteMode.OVERWRITE;
        }
        CsvTableSink cts = new CsvTableSink(filePath, fieldDelimiter, numFiles, mode);

        in.getTable().writeToSink(cts);
        return this;
    }
}
