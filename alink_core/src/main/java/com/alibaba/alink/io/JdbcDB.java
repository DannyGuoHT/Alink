package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.io.table.AlinkDbTable;
import com.alibaba.alink.io.table.JdbcTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@AlinkIONameAnnotation(name = "jdbc")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class JdbcDB extends AlinkDB {

    public static int MAX_VARCHAR_SIZE = 30000;

    public JdbcDB() {
        super(null);
    }

    public JdbcDB(String drivername, String dbUrl) {
        this(drivername, dbUrl, null, null);
    }

    public JdbcDB(String drivername, String dbUrl, String username, String password) {
        super(new AlinkParameter()
                .put("drivername", drivername)
                .put("dbUrl", dbUrl)
                .putIgnoreNull("username", username)
                .putIgnoreNull("password", password)
        );
        init(drivername, dbUrl, username, password);
    }

    protected void init(String drivername, String dbUrl, String username, String password) {
        this.drivername = drivername;
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;
    }

    private String username = null;
    private String password = null;
    private String drivername = null;
    private String dbUrl = null;

    private transient Connection conn = null;
    private transient Statement stmt = null;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDrivername() {
        return drivername;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    protected Statement getStmt()
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        if (null == stmt) {
            stmt = getConn().createStatement();
        }
        return stmt;
    }

    protected Connection getConn()
            throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        if (null == this.conn) {
            Class.forName(this.drivername).newInstance();
            if (null == username) {
                this.conn = DriverManager.getConnection(this.dbUrl);
            } else {
                this.conn = DriverManager.getConnection(this.dbUrl, this.username, this.password);
            }
        }
        return this.conn;
    }


    @Override
    public List <String> listTableNames()
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        DatabaseMetaData meta = getConn().getMetaData();
        ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"});
        ArrayList <String> tables = new ArrayList <>();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    public int executeUpdate(String sql)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return getStmt().executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return getStmt().execute(sql);
    }

    public ResultSet executeQuery(String query)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return getStmt().executeQuery(query);
    }

    @Override
    public boolean createTable(String tableName, TableSchema schema, AlinkParameter parameter)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        StringBuilder sbd = new StringBuilder();
        sbd.append("create table ").append(tableName).append(" (");
        String[] colNames = schema.getColumnNames();
        TypeInformation <?>[] colTypes = schema.getTypes();
        int n = colNames.length;
        for (int i = 0; i < n; i++) {
            String type = JdbcTypeConverter.getSqlType(colTypes[i]);
            if (type.toUpperCase().equals("VARCHAR")) {
                type += "(" + MAX_VARCHAR_SIZE + ")";
            }
            sbd.append(colNames[i]).append(" ").append(type);
            if (i < n - 1) {
                sbd.append(",");
            }
        }
        sbd.append(")");
        System.out.println(sbd.toString());
        return getStmt().execute(sbd.toString());
    }

    public boolean createTable(String tableName, TableSchema schema, String[] primaryKeys)
            throws Exception {
        throw new Exception("It is not support yet!");
    }

    @Override
    public boolean dropTable(String tableName)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        return getStmt().execute("DROP TABLE " + tableName);
    }

    public boolean hasTable(String table)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        boolean state = false;
        ResultSet rs = getConn().getMetaData().getTables(null, null, table.toUpperCase(), null);
        while (rs.next()) {
            state = true;
            break;
        }
        rs.close();
        return state;
    }

    @Override
    public boolean hasColumn(String tableName, String columnName)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        ResultSet rs = getStmt().executeQuery("select * from " + tableName);
        if (rs.findColumn(columnName) > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String[] getColNames(String tableName)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        ResultSetMetaData meta = null;
        try {
            meta = getStmt().executeQuery("select * from " + tableName + " limit 1").getMetaData();
        } catch (Exception ex) {
            meta = getStmt().executeQuery("select * from " + tableName).getMetaData();
        }
        int n = meta.getColumnCount();
        String[] colNames = new String[n];
        for (int i = 0; i < n; i++) {
            colNames[i] = meta.getColumnName(i + 1);
        }
        return colNames;
    }

    @Override
    public TableSchema getTableSchema(String tableName)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        //ResultSetMetaData meta = getStmt().executeQuery("select * from " + tableName).getMetaData();
        ResultSetMetaData meta = null;
        try {
            meta = getStmt().executeQuery("select * from " + tableName + " limit 1").getMetaData();
        } catch (Exception ex) {
            meta = getStmt().executeQuery("select * from " + tableName).getMetaData();
        }
        int n = meta.getColumnCount();
        String[] colNames = new String[n];
        TypeInformation <?>[] colTypes = new TypeInformation <?>[n];
        for (int i = 0; i < n; i++) {
            colNames[i] = meta.getColumnName(i + 1);
            int type = meta.getColumnType(i + 1);
            colTypes[i] = JdbcTypeConverter.getFlinkType(meta.getColumnType(i + 1));
        }
        return new TableSchema(colNames, colTypes);
    }

    public String[] getPrimaryKeys(String tableName) throws Exception {
        throw new Exception("It is not support yet.");

    }

    @Override
    public void close() throws SQLException {
        if (null != this.stmt) {
            this.stmt.close();
            this.stmt = null;
        }
        if (null != this.conn) {
            this.conn.close();
            this.conn = null;
//            try {
//                DriverManager.getConn(this.dbUrl + ";shutdown=true");
//            } catch (SQLException se) {
//                System.out.println("Database shut down normally");
//            }
        }
    }

    public void commit()
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        getConn().commit();
    }

    public void setAutoCommit(boolean autoCommit)
            throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        getConn().setAutoCommit(autoCommit);
    }

    protected void finalize() {
        try {
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public AlinkDbTable getDbTable(String tableName) throws Exception {
        return new JdbcTable(this, tableName);
    }

    @Override
    public String getName() {
        return this.dbUrl.split(":", 3)[2];
    }

    @Override
    public Table getStreamTable(String tableName, AlinkParameter parameter) throws Exception {
        TableSchema schema = getTableSchema(tableName);
        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setUsername(getUsername())
                .setPassword(getPassword())
                .setDrivername(getDrivername())
                .setDBUrl(getDbUrl())
                .setQuery("select * from " + tableName)
                .setRowTypeInfo(new RowTypeInfo(schema.getTypes(), schema.getColumnNames()))
                .finish();

        return RowTypeDataStream.toTable(
                AlinkSession.getStreamExecutionEnvironment().createInput(inputFormat),
                schema.getColumnNames(), schema.getTypes());
    }


    @Override
    public void sinkStream(String tableName, Table in, AlinkParameter parameter) {
        TableSchema schema = in.getSchema();
        String[] colNames = schema.getColumnNames();
        StringBuilder sbd = new StringBuilder();
        sbd.append("INSERT INTO ").append(tableName).append(" (").append(colNames[0]);
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append(colNames[i]);
        }
        sbd.append(") VALUES (?");
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append("?");
        }
        sbd.append(")");

        System.out.println(sbd.toString());

        JDBCAppendTableSink jdbcAppendTableSink = JDBCAppendTableSink.builder()
                .setUsername(getUsername())
                .setPassword(getPassword())
                .setDrivername(getDrivername())
                .setDBUrl(getDbUrl())
                .setQuery(sbd.toString())
                .setParameterTypes(schema.getTypes())
                .build();
        in.writeToSink(jdbcAppendTableSink);
    }

    @Override
    public Table getBatchTable(String tableName, AlinkParameter parameter) throws Exception {
        TableSchema schema = getTableSchema(tableName);
        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setUsername(getUsername())
                .setPassword(getPassword())
                .setDrivername(getDrivername())
                .setDBUrl(getDbUrl())
                .setQuery("select * from " + tableName)
                .setRowTypeInfo(new RowTypeInfo(schema.getTypes(), schema.getColumnNames()))
                .finish();

        return RowTypeDataSet.toTable(
                AlinkSession.getExecutionEnvironment().createInput(inputFormat),
                schema.getColumnNames(), schema.getTypes());
    }

    @Override
    public void sinkBatch(String tableName, Table in, AlinkParameter parameter) {
        TableSchema schema = in.getSchema();
        String[] colNames = schema.getColumnNames();
        StringBuilder sbd = new StringBuilder();
        sbd.append("INSERT INTO ").append(tableName).append(" (").append(colNames[0]);
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append(colNames[i]);
        }
        sbd.append(") VALUES (?");
        for (int i = 1; i < colNames.length; i++) {
            sbd.append(",").append("?");
        }
        sbd.append(")");

        JDBCAppendTableSink jdbcAppendTableSink = JDBCAppendTableSink.builder()
                .setUsername(getUsername())
                .setPassword(getPassword())
                .setDrivername(getDrivername())
                .setDBUrl(getDbUrl())
                .setQuery(sbd.toString())
                .setParameterTypes(schema.getTypes())
                .build();
        in.writeToSink(jdbcAppendTableSink);
    }
}

