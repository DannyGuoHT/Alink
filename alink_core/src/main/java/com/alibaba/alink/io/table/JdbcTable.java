package com.alibaba.alink.io.table;

import com.alibaba.alink.io.AlinkDB;
import com.alibaba.alink.io.JdbcDB;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;


public class JdbcTable extends AlinkDbTable {

    private JdbcDB jdbcDB;
    private String tableName;

    public JdbcTable(JdbcDB jdbcDB, String tableName) {
        this.jdbcDB = jdbcDB;
        this.tableName = tableName;

    }

    public String getOwner() {
        return this.jdbcDB.getUsername();
    }

    public String getTableName() {
        return this.tableName;
    }

    public String[] getColNames() {
        try {
            return this.jdbcDB.getTableSchema(this.tableName).getColumnNames();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public Class[] getColTypes() {
        try {
            TypeInformation <?>[] types = this.jdbcDB.getTableSchema(this.tableName).getTypes();
            Class[] comments = new Class[types.length];
            int i = 0;
            for (TypeInformation type : types) {
                if (type == BasicTypeInfo.DOUBLE_TYPE_INFO) {
                    comments[i] = Double.class;
                } else if (type == BasicTypeInfo.INT_TYPE_INFO ||
                        type == BasicTypeInfo.LONG_TYPE_INFO) {
                    comments[i] = Long.class;
                } else if (type == BasicTypeInfo.STRING_TYPE_INFO) {
                    comments[i] = String.class;
                } else {

                }
                i++;
            }
            return comments;
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public TableSchema getSchema() {
        try {
            return this.jdbcDB.getTableSchema(this.tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AlinkDB getAlinkDB() {
        return this.jdbcDB;
    }

}
