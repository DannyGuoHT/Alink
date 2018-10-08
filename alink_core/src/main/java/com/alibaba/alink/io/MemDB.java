package com.alibaba.alink.io;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.directreader.DirectReader;
import com.alibaba.alink.io.table.AlinkDbTable;
import com.alibaba.alink.common.utils.directreader.DistributedInfoProxy;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AlinkIONameAnnotation(name = "mem")
@AlinkDBAnnotation(tableNameAlias = "tableName")
public class MemDB extends AlinkDB {
    Map <String, Tuple2 <TableSchema, List <Row>>> data = new HashedMap();

    public MemDB(AlinkParameter params) {
        super(params);
    }

    @Override
    public List <String> listTableNames() throws Exception {
        return new ArrayList <>(data.keySet());
    }

    @Override
    public boolean execute(String sql) throws Exception {
        throw new Exception("Unsupported now.");
    }

    @Override
    public boolean createTable(String tableName, TableSchema schema, AlinkParameter parameter) throws Exception {
        if (data.containsKey(tableName)) {
            return false;
        } else {
            data.put(tableName, new Tuple2 <TableSchema, List <Row>>(schema, null));
            return true;
        }
    }

    @Override
    public boolean dropTable(String tableName) throws Exception {
        data.remove(tableName);
        return true;
    }

    @Override
    public boolean hasTable(String table) throws Exception {
        return data.containsKey(table);
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) throws Exception {
        TableSchema schema = getTableSchema(tableName);

        if (schema == null) {
            throw new Exception("Table has not scheme. Table name: " + tableName);
        }

        String[] colNames = schema.getColumnNames();

        if (TableUtil.findIndexFromName(colNames, columnName) >= 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String[] getColNames(String tableName) throws Exception {
        TableSchema schema = getTableSchema(tableName);

        if (schema == null) {
            throw new Exception("Table has not scheme. Table name: " + tableName);
        }

        return schema.getColumnNames();
    }

    @Override
    public TableSchema getTableSchema(String tableName) throws Exception {
        Tuple2 <TableSchema, List <Row>> tableData = data.get(tableName);

        if (tableData == null) {
            throw new Exception("It can not find table : " + tableName);
        }

        return tableData.f0;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public AlinkDbTable getDbTable(String tableName) throws Exception {
        throw new Exception("Unsupported now.");
    }

    @Override
    public Table getStreamTable(String tableName, AlinkParameter parameter) throws Exception {
        throw new Exception("Unsupported now.");
    }

    @Override
    public void sinkStream(String tableName, Table in, AlinkParameter parameter) {
        throw new RuntimeException("Unsupported now.");
    }

    @Override
    public Table getBatchTable(String tableName, AlinkParameter parameter) throws Exception {
        throw new Exception("Unsupported now.");
    }

    @Override
    public void sinkBatch(String tableName, Table in, AlinkParameter parameter) {
        throw new RuntimeException("Unsupported now.");
    }

    @Override
    public String getName() {
        return "mem db";
    }

    @Override
    public List <Row> directRead(DirectReader.BatchStreamConnector context) throws Exception {
        return context.getBuffer();
    }

    @Override
    public List <Row> directRead(DirectReader.BatchStreamConnector context, DistributedInfoProxy distributedInfoProxy) throws Exception {
        List <Row> rows = context.getBuffer();

        long start = distributedInfoProxy.startPos(rows.size());
        long cnt = distributedInfoProxy.localRowCnt(rows.size());

        return rows.subList((int) start, (int) (start + cnt));
    }

    @Override
    public void directWrite(String tableName, List <Row> data) throws Exception {
        if (hasTable(tableName)) {
            throw new Exception("Table has been exists. table name: " + tableName);
        }

        this.data.put(tableName, new Tuple2 <TableSchema, List <Row>>(null, data));
    }

    @Override
    public DirectReader.BatchStreamConnector initDirectReadContext(DirectReader.BatchStreamConnector connector) throws Exception {
        return connector;
    }
}
