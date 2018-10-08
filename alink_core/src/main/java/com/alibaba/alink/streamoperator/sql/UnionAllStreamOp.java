package com.alibaba.alink.streamoperator.sql;

import java.util.List;

import com.alibaba.alink.streamoperator.StreamOperator;

public class UnionAllStreamOp extends StreamOperator {

    public UnionAllStreamOp() {
        super(null);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new RuntimeException("Need more than 1 inputs!");
    }

    @Override
    public StreamOperator linkFrom(List <StreamOperator> ins) {
        if (null == ins || ins.size() <= 1) {
            throw new RuntimeException("Need more than 1 inputs!");
        }
        this.table = ins.get(0).getTable();
        for (int i = 1; i < ins.size(); i++) {
            this.table.unionAll(ins.get(i).getTable());
        }

        return this;
    }
}
