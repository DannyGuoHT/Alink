package com.alibaba.alink.io.table;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.batchoperator.source.DBSourceBatchOp;
import com.alibaba.alink.io.AlinkDB;
import org.apache.flink.table.api.TableSchema;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class AlinkDbTable {

    public List<String> listPartitionString() {
        return new ArrayList<>();
    }

    public String getComment() {
        return "";
    }

    public String getOwner() {
        return "";
    }

    public Date getCreatedTime() {
        return null;
    }

    public Date getLastDataModifiedTime() {
        return null;
    }

    public long getLife() {
        return -1;
    }


    public abstract String getTableName();

    public String[] getColComments() {
        String[] comments = new String[this.getColNames().length];
        for (int i = 0; i < comments.length; i++) {
            comments[i] = "";
        }
        return comments;
    }

    public abstract TableSchema getSchema();

    public abstract String[] getColNames();

    public abstract Class[] getColTypes();

    public int getColNum() {
        return getColNames().length;
    }

    public long getRowNum() throws Exception {
        DBSourceBatchOp op = new DBSourceBatchOp(getAlinkDB(), this.getTableName());
        return op.count();
    }

    public abstract AlinkDB getAlinkDB();

    public BatchOperator getBatchOperator() throws Exception {
        return new DBSourceBatchOp(this.getAlinkDB(), this.getTableName());
    }
}
