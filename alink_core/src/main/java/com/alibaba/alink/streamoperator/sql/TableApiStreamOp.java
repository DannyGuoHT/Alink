package com.alibaba.alink.streamoperator.sql;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.streamoperator.StreamOperator;

class TableApiStreamOp extends StreamOperator {

    public TableApiStreamOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        if (!this.params.contains("op")) {
            throw new RuntimeException("Must input parameter: op");
        }
        String tmpTableName = StreamOperator.createUniqueTableName();
        AlinkSession.getStreamTableEnvironment().registerTable(tmpTableName, in.getTable());

       switch (this.params.getString("op").toLowerCase()) {
            case "select":
                this.table = StreamOperator.sql("SELECT " + this.params.getString("param") + " FROM " + tmpTableName)
                    .getTable();
                break;
            case "as":
                this.table = in.getTable().as(this.params.getString("param"));
                break;
            case "where":
                this.table = StreamOperator.sql(
                    "SELECT * FROM " + tmpTableName + " WHERE " + this.params.getString("param")).getTable();
                break;
            case "filter":
                this.table = StreamOperator.sql(
                    "SELECT * FROM " + tmpTableName + " WHERE " + this.params.getString("filter")).getTable();
                break;
            default:
                throw new RuntimeException("Not support this operation : " + this.params.getString("op"));
        }

        return this;
    }

}
