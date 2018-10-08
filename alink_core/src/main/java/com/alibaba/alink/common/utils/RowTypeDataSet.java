package com.alibaba.alink.common.utils;

import com.alibaba.alink.common.AlinkSession;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public class RowTypeDataSet {

    public static DataSet <Row> fromTable(Table table) {
        return AlinkSession.getBatchTableEnvironment().toDataSet(table, Row.class);
    }

    public static Table toTable(DataSet <Row> data, TableSchema schema) {
        return toTable(data, schema.getColumnNames(), schema.getTypes());
    }

    public static Table toTable(DataSet <Row> data) {
        return toTable(data, new String[]{});
    }

    public static Table toTable(DataSet <Row> data, String[] colNames) {
        if (null == colNames || colNames.length == 0) {
            return AlinkSession.getBatchTableEnvironment().fromDataSet(data);
        } else {
            StringBuilder sbd = new StringBuilder();
            sbd.append(colNames[0]);
            for (int i = 1; i < colNames.length; i++) {
                sbd.append(",").append(colNames[i]);
            }
            return AlinkSession.getBatchTableEnvironment().fromDataSet(data, sbd.toString());
        }
    }

    public static Table toTable(DataSet <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
        try {
            return toTable(data, colNames);
        } catch (Exception ex) {
            if (null == colTypes) {
                throw ex;
            } else {
                DataSet <Row> t = getDataSetWithExplicitTypeDefine(data, colTypes);
                return toTable(t, colNames);
            }
        }
    }

    private static DataSet <Row> getDataSetWithExplicitTypeDefine(DataSet <Row> data, TypeInformation <?>[] colTypes) {
        DataSet <Row> r = data
                .map(
                        new MapFunction <Row, Row>() {
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
