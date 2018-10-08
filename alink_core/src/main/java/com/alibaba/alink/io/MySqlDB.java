package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;

import java.sql.DriverManager;

@AlinkIONameAnnotation(name = "mysql")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class MySqlDB extends JdbcDB {

    private String dbName;
    private String ip;
    private String port;

    public MySqlDB(String dbName, String ip, String port, String username, String password) {
        this.dbName = dbName;
        this.ip = ip;
        this.port = port;

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            DriverManager.getConnection(String.format("jdbc:mysql://%s:%s/%s", ip, port, dbName), username, password);
        } catch (Exception ex) {

        }
        init("com.mysql.jdbc.Driver", String.format("jdbc:mysql://%s:%s/%s", ip, port, dbName), username, password);

        this.params
                .put("dbName", dbName)
                .put("ip", ip)
                .put("port", port)
                .put("username", username)
                .put("password", password);

        this.dbName = dbName;
    }

    public MySqlDB(AlinkParameter alinkParameter) {
        this(alinkParameter.getString("dbName"),
                alinkParameter.getString("ip"),
                alinkParameter.getString("port"),
                alinkParameter.getString("username"),
                alinkParameter.getString("password"));
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
