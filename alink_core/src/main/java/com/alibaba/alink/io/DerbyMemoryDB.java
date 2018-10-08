package com.alibaba.alink.io;

import java.sql.DriverManager;
import java.sql.SQLException;

@AlinkIONameAnnotation(name = "derbymem")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class DerbyMemoryDB extends JdbcDB {

    public DerbyMemoryDB(String dbName)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            DriverManager.getConnection(String.format("jdbc:derby:memory:%s", dbName));
        } catch (Exception ex) {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            DriverManager.getConnection(String.format("jdbc:derby:memory:%s;create=true", dbName));
        }
        init("org.apache.derby.jdbc.EmbeddedDriver", String.format("jdbc:derby:memory:%s", dbName),null,null);
    }

    public static void createDatabase(String dbName)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        DriverManager.getConnection(String.format("jdbc:derby:memory:%s;create=true", dbName));
    }
}
