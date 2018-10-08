package com.alibaba.alink.io.jdbc;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.io.JdbcDB;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class JDBCUpserSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final long serialVersionUID = 1L;

    private JdbcDB jdbcDB;
    private String tableName;
    private String[] primaryColNames;
    private TypeInformation[] primaryColTypes;
    private String[] otherColNames; //colnames without primary col
    private TypeInformation[] otherColTypes;
    private String[] colNames;
    private TypeInformation[] colTypes;

    private int N;



    public JDBCUpserSinkFunction(JdbcDB jdbcDB, String tableName, String[] primaryColNames) {
        this.jdbcDB = jdbcDB;
        this.tableName = tableName;
        this.primaryColNames = primaryColNames;
        try {
            TableSchema schema = this.jdbcDB.getTableSchema(tableName);
            colNames = schema.getColumnNames();
            colTypes = schema.getTypes();
            N = colNames.length;
            if (primaryColNames != null && primaryColNames.length != 0) {
                for (int i = 0; i < primaryColNames.length; i++) {
                    if (TableUtil.findIndexFromName(colNames, primaryColNames[i]) < 0) {
                        throw new RuntimeException("primary col " + primaryColNames[i] + " not exsit.");
                    }
                }
                List<String> otherCols = new ArrayList<>();
                for (int i = 0; i < colNames.length; i++) {
                    if (TableUtil.findIndexFromName(primaryColNames, colNames[i]) < 0) {
                        otherCols.add(colNames[i]);
                    }
                }
                this.otherColNames = otherCols.toArray(new String[0]);
                if (this.otherColNames.length == 0) {
                    throw new RuntimeException("primary col names must be not all table colnames.");
                }

                this.primaryColTypes = new TypeInformation[this.primaryColNames.length];
                for (int i = 0; i < this.primaryColNames.length; i++) {
                    this.primaryColTypes[i] = this.colTypes[TableUtil.findIndexFromName(colNames, this.primaryColNames[i])];
                }
                this.otherColTypes = new TypeInformation[this.otherColNames.length];
                for (int i = 0; i < this.otherColNames.length; i++) {
                    this.otherColTypes[i] = this.colTypes[TableUtil.findIndexFromName(colNames, otherColNames[i])];
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }


    @Override
    public void invoke(Tuple2<Boolean, Row> tuple2) throws Exception {
        if (tuple2.f0 = false) {
            return;
        }
        //"insert into ning_test values('test', 'aa')"
        //"update  ning_test set f1='cc' where f0='testx'"
        Row row = tuple2.f1;

        if (primaryColNames == null || primaryColNames.length == 0) {
            jdbcDB.executeQuery(getInsertSql(row));
        } else {
            int count = jdbcDB.executeUpdate(getUpdataSql(row));
            if (count == 0) {
                jdbcDB.executeQuery(getInsertSql(row));
            }
        }
    }

    private String convert2String(Object val, TypeInformation type) {
        if (type == Types.STRING) {
            return "'" + val + "'";
        } else {
            return String.valueOf(val);
        }
    }

    //"insert into ning_test values('test', 'aa')"
    private String getInsertSql(Row row) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ")
                .append(tableName)
                .append(" values(")
                .append(convert2String(row.getField(0), colTypes[0]));
        for (int i = 1; i < N; i++) {
            sb.append(", ");
            sb.append(convert2String(row.getField(i), colTypes[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    //"update  ning_test set f1='cc' where f0='testx'"
    private String getUpdataSql(Row row) {
        StringBuilder sb = new StringBuilder();
        int idx = TableUtil.findIndexFromName(colNames, this.otherColNames[0]);
        sb.append("update ").append(this.tableName).append(" set ")
                .append(otherColNames[0]).
                append("=").
                append(convert2String(row.getField(idx), colTypes[idx]));
        for (int i = 0; i < otherColNames.length; i++) {
            idx = TableUtil.findIndexFromName(colNames, this.otherColNames[i]);
            sb.append(", ")
                    .append(otherColNames[i]).
                    append("=").
                    append(convert2String(row.getField(idx), colTypes[idx]));
        }

        idx = TableUtil.findIndexFromName(colNames, this.primaryColNames[0]);
        sb.append(" where ")
                .append(primaryColNames[0]).append("=").append(convert2String(row.getField(idx), colTypes[idx]));

        for (int i = 0; i < otherColNames.length; i++) {
            idx = TableUtil.findIndexFromName(colNames, this.otherColNames[i]);
            sb.append(" and  ").
                    append(primaryColNames[i]).
                    append("=").
                    append(convert2String(row.getField(idx), colTypes[idx]));
        }

        return sb.toString();
    }

    protected void finalize() {
        try {
            jdbcDB.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
}