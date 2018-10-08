package com.alibaba.alink.common.utils;

import com.alibaba.alink.common.AlinkSession;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public class RowTypeDataStream {

    public static DataStream<Row> fromTable(Table table) {
        //return AlinkSession.getStreamTableEnvironment().toDataStream(table.select("*"), Row.class);
        return AlinkSession.getStreamTableEnvironment().toAppendStream(table, Row.class);
    }

    public static Table toTable(DataStream<Row> data, TableSchema schema) {
        return toTable(data, schema.getColumnNames(), schema.getTypes());
    }

    public static Table toTable(DataStream<Row> data) {
        return toTable(data, new String[]{});
    }

    public static Table toTable(DataStream<Row> data, String[] colNames) {
        if (null == colNames || colNames.length == 0) {
            return AlinkSession.getStreamTableEnvironment().fromDataStream(data);
        } else {
            StringBuilder sbd = new StringBuilder();
            sbd.append(colNames[0]);
            for (int i = 1; i < colNames.length; i++) {
                sbd.append(",").append(colNames[i]);
            }
            return AlinkSession.getStreamTableEnvironment().fromDataStream(data, sbd.toString());
        }
    }

    public static Table toTable(DataStream<Row> data, String[] colNames, TypeInformation<?>[] colTypes) {
        try {
            return toTable(data, colNames);
        } catch (Exception ex) {
            if (null == colTypes) {
                throw ex;
            } else {
                DataStream<Row> t = getDataSetWithExplicitTypeDefine(data, colTypes);
                return toTable(t, colNames);
            }
        }
    }

    public static Table toTableUsingStreamEnv(DataStream<Row> data, String[] colNames, TypeInformation<?>[] colTypes) {
        if (null == colTypes) {
            throw new RuntimeException("col type is null");
        }

        DataStream<Row> t = getDataSetWithExplicitTypeDefine(data, colTypes);

        return toTable(t, colNames);
    }

    private static DataStream<Row> getDataSetWithExplicitTypeDefine(DataStream<Row> data, TypeInformation<?>[] colTypes) {
        DataStream<Row> r = data
                .map(
                        new MapFunction<Row, Row>() {
                            @Override
                            public Row map(Row t) throws Exception {
                                return t;
                            }
                        }
                )
                .returns(new RowTypeInfo(colTypes));

        return r;
    }
}
