package com.alibaba.alink.batchoperator.sql;

import java.util.List;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;

import org.apache.flink.table.api.Table;

class TableApiBatchOp extends BatchOperator {

    public TableApiBatchOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public BatchOperator linkFrom(List <BatchOperator> ins) {
        if (ins.size() == 1) {
            return linkFrom(ins.get(0));
        } else if (ins.size() == 2) {
            String op = this.params.getString("op").toLowerCase();
            String joinType = null;
            switch (op) {
                case "join":
                    joinType = " INNER JOIN ";
                    break;
                case "leftouterjoin":
                    joinType = " LEFT JOIN ";
                    break;
                case "rightouterjoin":
                    joinType = " RIGHT JOIN ";
                    break;
                case "fullouterjoin":
                    joinType = " FULL OUTER JOIN ";
                    break;
                default:
                    throw new RuntimeException("Not support this sql operation : " + this.params.getString("op"));
            }

            String tmpTableName1 = BatchOperator.createUniqueTableName();
            AlinkSession.getBatchTableEnvironment().registerTable(tmpTableName1, ins.get(0).getTable());
            String tmpTableName2 = BatchOperator.createUniqueTableName();
            AlinkSession.getBatchTableEnvironment().registerTable(tmpTableName2, ins.get(1).getTable());

            this.table = BatchOperator.sql(
                "SELECT " + this.params.getStringOrDefault("selectClause", "*")
                    + " FROM " + tmpTableName1 + " as a "
                    + joinType + tmpTableName2 + " as b "
                    + " ON " + this.params.getString("whereClause")
            ).getTable();

            return this;
        } else {
            throw new RuntimeException("Need one or two inputs");
        }
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String op = this.params.getString("op").toLowerCase();
        Table t = in.getTable();
        String tmpTableName = BatchOperator.createUniqueTableName();
        AlinkSession.getBatchTableEnvironment().registerTable(tmpTableName, in.getTable());
        switch (op) {
            case "select":
                this.table = BatchOperator.sql("SELECT " + this.params.getString("param") + " FROM " + tmpTableName)
                    .getTable();
                break;
            case "as":
                this.table = in.getTable().as(this.params.getString("param"));
                break;
            case "where":
                this.table = BatchOperator.sql(
                    "SELECT * FROM " + tmpTableName + " WHERE " + this.params.getString("param")).getTable();
                break;
            case "filter":
                this.table = BatchOperator.sql(
                    "SELECT * FROM " + tmpTableName + " WHERE " + this.params.getString("param")).getTable();
                break;
            case "orderby":
                String s = "SELECT * FROM " + tmpTableName + " ORDER BY " + this.params.getString("param");
                if (this.params.contains("limit") && null != this.params.getInteger("limit")) {
                    s += " LIMIT " + this.params.getInteger("limit");
                }
                if (this.params.contains("offset") && null != this.params.getInteger("offset")) {
                    s += " OFFSET " + this.params.getInteger("offset");
                }
                if (this.params.contains("fetch") && null != this.params.getInteger("fetch")) {
                    s += " FETCH " + this.params.getInteger("fetch");
                }
                this.table = BatchOperator.sql(s).getTable();
                break;
            case "distinct":
                this.table = BatchOperator.sql("SELECT DISTINCT * FROM " + tmpTableName).getTable();
                break;
            case "groupby":
                this.table = BatchOperator.sql(
                    "SELECT " + this.params.getString("selectClause") + " FROM " + tmpTableName + " GROUP BY "
                        + this.params.getString("groupByClause")).getTable();
                break;
            default:
                throw new RuntimeException("Not support this sql operation : " + this.params.getString("op"));
        }
        return this;
    }

}
