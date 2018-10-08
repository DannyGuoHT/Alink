package com.alibaba.alink.streamoperator.sql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.streamoperator.StreamOperator;

public class SqlCmdStreamOp extends StreamOperator {
    private String[] alias;
    private String command;
    public final static Integer INPUT_NUM = 5;

    public SqlCmdStreamOp(AlinkParameter params) {
        super(params);
        this.alias = super.params.getStringArray("alias");
        this.command = super.params.getString("command");
        if (isDuplicated()) {
            throw new RuntimeException("alias are duplicated.");
        }
    }

    public SqlCmdStreamOp(String[] alias,
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
    public StreamOperator linkFrom(StreamOperator in) {
        List <StreamOperator> ins = new ArrayList <>(INPUT_NUM);
        ins.add(in);
        for (int i = 0; i < INPUT_NUM - 1; i++) {
            ins.add(null);
        }
        return linkFrom(ins);
    }

    @Override
    public StreamOperator linkFrom(List <StreamOperator> ins) {
        if (ins == null || ins.size() != INPUT_NUM) {
            throw new RuntimeException("Need " + INPUT_NUM.toString() + " inputs.");
        }
        for (int i = 0; i < INPUT_NUM; ++i) {
            if (ins.get(i) != null) {
                ins.get(i).forceSetTableName(alias[i]);
            }
        }
        this.table = StreamOperator.sql(command).getTable();
        return this;
    }
}
