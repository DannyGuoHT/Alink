package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;

import java.util.List;

public class UnionAllBatchOp extends BatchOperator {

    public UnionAllBatchOp() {
        this(new AlinkParameter());
    }

    public UnionAllBatchOp(AlinkParameter param) {
        super(param);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator op) {
        throw new RuntimeException("Need more than 1 inputs!");
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        if (null == ins || ins.size() <= 1) {
            throw new RuntimeException("Need more than 1 inputs!");
        }
        this.table = ins.get(0).getTable();
        for (int i = 1; i < ins.size(); i++) {
            this.table = this.table.unionAll(ins.get(i).getTable());
        }

        return this;
    }
}
