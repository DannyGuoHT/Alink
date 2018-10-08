package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.io.table.AlinkDbTable;
import com.alibaba.alink.common.utils.directreader.DistributedInfoProxy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Set;

public abstract class AlinkDB implements AlinkSerializable {

    protected AlinkParameter params;

    protected AlinkDB(AlinkParameter params) {
        if (null == params) {
            this.params = new AlinkParameter();
        } else {
            this.params = params.clone();
        }
        this.params.put(ParamName.alinkIOName, AnnotationUtils.annotationName(this.getClass()));
    }

    public abstract List <String> listTableNames() throws Exception;

    public abstract boolean execute(String sql) throws Exception;

    public boolean createTable(String tableName, TableSchema schema) throws Exception {
        return createTable(tableName, schema, null);
    }

    public boolean createTable(String tableName, AlinkParameter parameter) throws Exception {
        return createTable(tableName, getTableSchema(parameter), parameter);
    }

    public abstract boolean createTable(String tableName, TableSchema schema, AlinkParameter parameter) throws Exception;

    public abstract boolean dropTable(String tableName) throws Exception;

    public abstract boolean hasTable(String table) throws Exception;

    public abstract boolean hasColumn(String tableName, String columnName) throws Exception;

    public abstract String[] getColNames(String tableName) throws Exception;

    public abstract TableSchema getTableSchema(String tableName) throws Exception;

    public TableSchema getTableSchema(AlinkParameter parameter) {
        String tableSchmaStr = parameter.getString("tableSchema");
        if (tableSchmaStr == null || tableSchmaStr.length() == 0) {
            throw new RuntimeException("table schema is empty.");
        }

        String[] kvs = tableSchmaStr.split(",");
        String[] colNames = new String[kvs.length];
        TypeInformation <?>[] colTypes = new TypeInformation <?>[kvs.length];

        for (int i = 0; i < kvs.length; i++) {
            String[] splits = kvs[i].split(" ");
            if (splits.length != 2) {
                throw new RuntimeException("table schema error. " + tableSchmaStr);
            }
            colNames[i] = splits[0];
            switch (splits[1].trim().toLowerCase()) {
                case "string":
                    colTypes[i] = Types.STRING;
                    break;
                case "double":
                    colTypes[i] = Types.DOUBLE;
                    break;
                case "long":
                    colTypes[i] = Types.LONG;
                    break;
                case "boolean":
                    colTypes[i] = Types.BOOLEAN;
                    break;
                case "timestamp":
                    colTypes[i] = Types.SQL_TIMESTAMP;
                    break;
                default:
                    break;
            }
        }

        return new TableSchema(colNames, colTypes);
    }

    public abstract void close() throws Exception;

    public abstract AlinkDbTable getDbTable(String tableName) throws Exception;

    public abstract Table getStreamTable(String tableName, AlinkParameter parameter) throws Exception;

    public abstract void sinkStream(String tableName, Table in, AlinkParameter parameter);

    public abstract Table getBatchTable(String tableName, AlinkParameter parameter) throws Exception;

    public abstract void sinkBatch(String tableName, Table in, AlinkParameter parameter);

    public abstract String getName();

    public List <Row> directRead(DirectReader.BatchStreamConnector connector) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }

    public List <Row> directRead(DirectReader.BatchStreamConnector connector, DistributedInfoProxy distributedInfoProxy) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }


    public void directWrite(String tableName, List <Row> data) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }

    public DirectReader.BatchStreamConnector initDirectReadContext(DirectReader.BatchStreamConnector connector) throws Exception {
        throw new Exception("Unsupported direct read now.");
    }

    public AlinkParameter getParams() {
        return this.params.clone();
    }

    public static AlinkDB of(AlinkParameter params) throws Exception {
        if (AlinkDB.isAlinkDB(params)) {
            return AnnotationUtils.createDB(params.getString(ParamName.alinkIOName), params);
        } else {
            throw new RuntimeException("NOT a DB parameter.");
        }
    }

    public static boolean isAlinkDB(AlinkParameter params) {
        if (params.contains(ParamName.alinkIOName)) {
            return AnnotationUtils.isAlinkDB(params.getString(ParamName.alinkIOName));
        } else {
            return false;
        }
    }

    public static void main(String[] args) {
        Set <Class <?>> all = AnnotationUtils.allAnntated("com.alibaba.alink", AlinkIONameAnnotation.class);

        for (Class <?> cls : all) {
            System.out.println(cls.getName());
        }
    }
}

