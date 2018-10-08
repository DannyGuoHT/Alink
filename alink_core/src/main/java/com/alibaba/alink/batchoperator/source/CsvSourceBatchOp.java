package com.alibaba.alink.batchoperator.source;

import java.net.URL;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.io.AlinkIONameAnnotation;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.http.HttpCsvTableSource;
import com.alibaba.alink.io.utils.CsvUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;


@AlinkIONameAnnotation(name = "csv")
public class CsvSourceBatchOp extends AlinkSourceBatchOp {

    public CsvSourceBatchOp(AlinkParameter params) {
        super(AnnotationUtils.annotationName(CsvSourceBatchOp.class), params);

        String filePath = params.getString("filePath");
        String schemaStr;
        if (params.contains("schemaStr")) {
            schemaStr = params.getString("schemaStr");
        } else if (params.contains("schema")) {
            // for backward compatibility
            schemaStr = params.getString("schema");
        } else {
            throw new IllegalArgumentException("Missing param \"schemaStr\"");
        }
        String fieldDelim = params.getStringOrDefault("fieldDelimiter", ",");
        String rowDelim = params.getStringOrDefault("rowDelimiter", "\n");

        fieldDelim = CsvUtil.unEscape(fieldDelim);
        rowDelim = CsvUtil.unEscape(rowDelim);

        String[] colNames = CsvUtil.getColNames(schemaStr);
        TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);

        this.init(TableUtil.getTempTableName(), filePath, colNames, colTypes, fieldDelim, rowDelim);
    }

    public CsvSourceBatchOp(String filePath, TableSchema schema) {
        this(new AlinkParameter()
            .put("filePath", filePath)
            .put("schemaStr", TableUtil.schema2SchemaStr(schema))
        );
    }

    public CsvSourceBatchOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes) {
        this(new AlinkParameter()
            .put("filePath", filePath)
            .put("schemaStr", TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
        );
    }

    public CsvSourceBatchOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes,
                            String fieldDelim, String rowDelim) {
        this(new AlinkParameter()
            .put("filePath", filePath)
            .put("schemaStr", TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
            .put("fieldDelimiter", fieldDelim)
            .put("rowDelimiter", rowDelim)
        );
    }

    private void init(String tableName, String filePath, String[] colNames, TypeInformation <?>[] colTypes,
                      String fieldDelim, String rowDelim) {
        String protocol = null;
        URL url = null;

        try {
            url = new URL(filePath);
            protocol = url.getProtocol();
        } catch (Exception e) {
            protocol = "";
        }

        if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
            HttpCsvTableSource csvTableSource = new HttpCsvTableSource(
                filePath, colNames, colTypes, fieldDelim, rowDelim,
                null, // quoteCharacter
                false, // ignoreFirstLine
                null, //"%",    // ignoreComments
                false); // lenient
            AlinkSession.getBatchTableEnvironment().registerTableSource(tableName, csvTableSource);
            this.table = AlinkSession.getBatchTableEnvironment().scan(tableName);
        } else {
            CsvTableSource csvTableSource = new CsvTableSource(
                filePath, colNames, colTypes, fieldDelim, rowDelim,
                null, // quoteCharacter
                false, // ignoreFirstLine
                null, //"%",    // ignoreComments
                false); // lenient
            AlinkSession.getBatchTableEnvironment().registerTableSource(tableName, csvTableSource);
            this.table = AlinkSession.getBatchTableEnvironment().scan(tableName);
        }

        // Fix bug: This is a workaround to deal with the bug of SQL planner.
        // In this bug, CsvSource linked to UDTF  will throw an exception:
        // "Cannot generate a valid execution plan for the given query".
        DataSet <Row> out = RowTypeDataSet.fromTable(this.table)
            .map(new MapFunction <Row, Row>() {
                @Override
                public Row map(Row value) throws Exception {
                    return value;
                }
            });

        this.table = RowTypeDataSet.toTable(out, colNames, colTypes);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new RuntimeException("This is source batch op, should not call linkFrom.");
    }
}
