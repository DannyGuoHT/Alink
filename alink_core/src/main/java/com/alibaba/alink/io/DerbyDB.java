package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;

import java.sql.DriverManager;
import java.sql.SQLException;

@AlinkIONameAnnotation(name = "derby")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class DerbyDB extends JdbcDB {

    private String dbName;

    public DerbyDB(String dbName) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        this(dbName, null, null);

        this.dbName = dbName;
    }

    public DerbyDB(String dbName, String username, String password) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {

        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            DriverManager.getConnection(String.format("jdbc:derby:%s", dbName), username, password);
        } catch (Exception ex) {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", dbName), username, password);
        }
        init("org.apache.derby.jdbc.EmbeddedDriver", String.format("jdbc:derby:%s", dbName), username, password);

        this.params
                .put("dbName", dbName)
                .putIgnoreNull("username", username)
                .putIgnoreNull("password", password);

        this.dbName = dbName;
    }

    public DerbyDB(AlinkParameter alinkParameter) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        this(alinkParameter.getString("dbName"),
                alinkParameter.getStringOrDefault("username", null),
                alinkParameter.getStringOrDefault("password", null));
    }

    public String getDbName() {
        return dbName;
    }

    public static void createDatabase(String dbName)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        DriverManager.getConnection(String.format("jdbc:derby:%s;create=true", dbName));
    }
}
