package com.alibaba.alink.batchoperator.sql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;

public class SqlCmdBatchOp extends BatchOperator {
    private String[] alias;
    private String command;
    public final static Integer INPUT_NUM = 5;

    public SqlCmdBatchOp(AlinkParameter params) {
        super(params);
        this.alias = super.params.getStringArray("alias");
        this.command = super.params.getString("command");
        if (isDuplicated()) {
            throw new RuntimeException("alias are duplicated.");
        }
    }

    public SqlCmdBatchOp(String[] alias,
                         String command) throws Exception {
        super(null);
        this.alias = alias;
        this.command = command;
        if (isDuplicated()) {
            throw new Exception("alias are duplicated.");
        }
    }

    private boolean isDuplicated() {
        Set <String> nameSet = new HashSet <>();
        for (int i = 0; i < alias.length; ++i) {
            if (alias[i] != null && nameSet.contains(alias[i])) {
                return true;
            }
            nameSet.add(alias[i]);
        }
        return false;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new RuntimeException("Need to input a list.");
    }

    @Override
    public BatchOperator linkFrom(List <BatchOperator> ins) {
        if (ins == null || ins.size() != INPUT_NUM) {
            throw new RuntimeException("Need " + INPUT_NUM.toString() + " inputs.");
        }
        for (int i = 0; i < INPUT_NUM; ++i) {
            if (ins.get(i) != null) {
                ins.get(i).forceSetTableName(alias[i]);
            }
        }
        this.table = BatchOperator.sql(command).getTable();
        return this;
    }
}























