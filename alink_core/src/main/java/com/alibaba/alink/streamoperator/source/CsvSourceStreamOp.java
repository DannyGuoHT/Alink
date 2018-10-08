package com.alibaba.alink.streamoperator.source;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.io.AlinkIONameAnnotation;
import com.alibaba.alink.io.http.HttpCsvTableSource;
import com.alibaba.alink.io.AnnotationUtils;
import com.alibaba.alink.io.utils.CsvUtil;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.net.URL;


@AlinkIONameAnnotation(name = "csv")
public class CsvSourceStreamOp extends AlinkSourceStreamOp {
    public CsvSourceStreamOp(AlinkParameter params) {
        super(AnnotationUtils.annotationName(CsvSourceStreamOp.class), params);

        String filePath = params.getString("filePath");
        String schemaStr;
        if (params.contains("schemaStr"))
            schemaStr = params.getString("schemaStr");
        else if (params.contains("schema")) // for backward compatibility
            schemaStr = params.getString("schema");
        else
            throw new IllegalArgumentException("Missing param \"schemaStr\"");
        String fieldDelim = params.getStringOrDefault("fieldDelimiter", ",");
        String rowDelim = params.getStringOrDefault("rowDelimiter", "\n");

        fieldDelim = CsvUtil.unEscape(fieldDelim);
        rowDelim = CsvUtil.unEscape(rowDelim);

        String[] colNames = CsvUtil.getColNames(schemaStr);
        TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);

        this.init(TableUtil.getTempTableName(), filePath, colNames, colTypes, fieldDelim, rowDelim);
    }

    public CsvSourceStreamOp(String filePath, TableSchema schema) {
        this(new AlinkParameter()
                .put("filePath", filePath)
                .put("schemaStr", TableUtil.schema2SchemaStr(schema))
        );
    }

    public CsvSourceStreamOp(String filePath, String[] colNames, TypeInformation<?>[] colTypes) {
        this(new AlinkParameter()
                .put("filePath", filePath)
                .put("schemaStr", TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
        );
    }

    public CsvSourceStreamOp(String filePath, String[] colNames, TypeInformation<?>[] colTypes,
                             String fieldDelim, String rowDelim) {
        this(new AlinkParameter()
                .put("filePath", filePath)
                .put("schemaStr", TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
                .put("fieldDelimiter", fieldDelim)
                .put("rowDelimiter", rowDelim)
        );
    }

    public void init(String tableName, String filePath, String[] colNames, TypeInformation<?>[] colTypes,
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
            AlinkSession.getStreamTableEnvironment().registerTableSource(tableName, csvTableSource);
            this.table = AlinkSession.getStreamTableEnvironment().scan(tableName);
        } else {
            CsvTableSource csvTableSource = new CsvTableSource(
                    filePath, colNames, colTypes, fieldDelim, rowDelim,
                    null, // quoteCharacter
                    false, // ignoreFirstLine
                    null, //"%",    // ignoreComments
                    false); // lenient
            AlinkSession.getStreamTableEnvironment().registerTableSource(tableName, csvTableSource);
            this.table = AlinkSession.getStreamTableEnvironment().scan(tableName);
        }


        // Fix bug: This is a workaround to deal with the bug of SQL planner.
        // In this bug, CsvSource linked to UDTF  will throw an exception:
        // "Cannot generate a valid execution plan for the given query".
        DataStream<Row> out = RowTypeDataStream.fromTable(this.table)
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        return value;
                    }
                });

        this.table = RowTypeDataStream.toTable(out, colNames, colTypes);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

//    public static DataStream<Row> CreateCSVFileSource(String filePath, String fieldSeperator, TableSchema info) {
//
//        TextInputFormat format = new TextInputFormat(new Path(filePath));
//        format.setFilesFilter(FilePathFilter.createDefaultFilter());
//        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
//        format.setFilePath(filePath);
//        String sourceName = "Custom File Source";
//        long interval = 100;
//        ContinuousFileMonitoringFunction<String> monitoringFunction = new ContinuousFileMonitoringFunction(format,
//                //FileProcessingMode.PROCESS_CONTINUOUSLY,
//                FileProcessingMode.PROCESS_ONCE,
//                AlinkSession.getStreamExecutionEnvironment().getParallelism(),
//                interval);
//
//        XContinuousFileReaderOperator<String> reader = new XContinuousFileReaderOperator(format);
//
//        SingleOutputStreamOperator<String> op = AlinkSession.getStreamExecutionEnvironment().addSource(monitoringFunction, sourceName).transform("Split Reader: " + sourceName, typeInfo, reader);
//        org.apache.flink.streaming.api.datastream.DataStreamSource<String> source = new org.apache.flink.streaming.api.datastream.DataStreamSource(op);
//
//        DataStream<Row> sourceStream = source.flatMap(new CSVTokenizer(fieldSeperator, info));
//        return sourceStream;
//    }

//    private static final class CSVTokenizer implements FlatMapFunction<String, Row> {
//
//        private Class[] colTypes;
//        private int nTypes;
//        private String fieldSeperator;
//
//        public CSVTokenizer(String fieldSeperator, TableSchema info) {
//            this.nTypes = info.getColumnNames().length;
//            this.fieldSeperator = fieldSeperator;
//            this.colTypes = new Class[nTypes];
//            for (int i = 0; i < nTypes; i++) {
//                colTypes[i] = info.getTypes()[i].getTypeClass();
//            }
//        }
//
//        @Override
//        public void flatMap(String value, Collector<Row> out)
//                throws Exception {
//            String[] tokens = value.split(this.fieldSeperator);
//
//            Row r = new Row(nTypes);
//
//            if (nTypes <= tokens.length) {
//                for (int i = 0; i < nTypes; i++) {
//                    if (Double.class == this.colTypes[i]) {
//                        r.setField(i, Double.valueOf(tokens[i]));
//                    } else if (Long.class == this.colTypes[i]) {
//                        r.setField(i, Long.valueOf(tokens[i]));
//                    } else if (String.class == this.colTypes[i]) {
//                        r.setField(i, tokens[i]);
//                    } else if (Float.class == this.colTypes[i]) {
//                        r.setField(i, Float.valueOf(tokens[i]));
//                    } else if (Integer.class == this.colTypes[i]) {
//                        r.setField(i, Integer.valueOf(tokens[i]));
//                    } else {
//                        throw new RuntimeException("Not supported data class!");
//                    }
//                }
//                out.collect(r);
//            }
//        }
//    }
//
//    private Table fromHttpCsv(URL url, String[] colNames, TypeInformation<?>[] colTypes,
//                              String fieldDelim, String rowDelim, boolean ignoreFirstLine) {
//        if (!rowDelim.equals("\n"))
//            throw new RuntimeException("DO NOT support row delimiter other than " + "\"\\n\"");
//
//        DataStream<Integer> seed = AlinkSession.getStreamExecutionEnvironment()
//                .fromCollection(Arrays.asList(0));
//
//        DataStream<Row> rows = seed.flatMap(new CsvUtil.HttpReader(url, colTypes, fieldDelim, rowDelim, ignoreFirstLine)).setParallelism(1);
//        return RowTypeDataStream.toTable(rows, colNames, colTypes);
//    }
}
