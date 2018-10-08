package com.alibaba.alink.io;


import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@AlinkIONameAnnotation(name = "sqlite")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class SQLiteDB extends JdbcDB {

    private String dbName;

    public SQLiteDB(String dbName) {
        super("org.sqlite.JDBC", String.format("jdbc:sqlite:%s", dbName));
        this.dbName = dbName;
    }

    public SQLiteDB(String dbName, String username, String password) {
        super("org.sqlite.JDBC", String.format("jdbc:sqlite:%s", dbName), username, password);

        this.params.put("dbName", dbName)
                .putIgnoreNull("username", username)
                .putIgnoreNull("password", password);

        this.dbName = dbName;
    }

    public SQLiteDB(AlinkParameter alinkParameter) {
        this(alinkParameter.getString("dbName"),
                alinkParameter.getStringOrDefault("username", null),
                alinkParameter.getStringOrDefault("password", null));
    }

    public String getDbName() {
        return dbName;
    }


    public boolean createTable(String tableName, TableSchema schema, String[] primaryKeys)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {

        StringBuilder sbd = new StringBuilder();
        sbd.append("create table ").append(tableName).append(" (");
        String[] colNames = schema.getColumnNames();
        TypeInformation <?>[] colTypes = schema.getTypes();
        int n = colNames.length;
        for (int i = 0; i < n; i++) {
            String type = JdbcTypeConverter.getSqlType(colTypes[i]);
            if (type.toUpperCase().equals("VARCHAR")) {
                type += "(30000)";
            }
            sbd.append(colNames[i]).append(" ").append(type);
            if (i < n - 1) {
                sbd.append(",");
            }
        }

        if (primaryKeys != null && primaryKeys.length != 0) {
            sbd.append(", ");
            sbd.append(" PRIMARY KEY (");
            sbd.append(primaryKeys[0]);
            for (int i = 1; i < primaryKeys.length; i++) {
                sbd.append(",");
                sbd.append(primaryKeys[i]);
            }
            sbd.append(")");
        }
        sbd.append(")");


        System.out.println(sbd.toString());
        return getStmt().execute(sbd.toString());
    }

    public String[] getPrimaryKeys(String tableName) throws Exception {
        List <String> primaryKeysList = new ArrayList <>();
        ResultSet rs = getConn().getMetaData().getPrimaryKeys(null, null, tableName);

        while (rs.next()) {
            primaryKeysList.add(rs.getString("COLUMN_NAME"));
        }
        return primaryKeysList.toArray(new String[0]);

    }
}
