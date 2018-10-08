package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;

import java.util.List;

public class UnionBatchOp extends BatchOperator {

    private String type;

    public UnionBatchOp() {
        this(new AlinkParameter());
    }

    public UnionBatchOp(AlinkParameter param) {
        super(param);
        this.type = super.params.getStringOrDefault("type", "union");
    }

    public UnionBatchOp(String type) {
        super(null);
        this.type = type;
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

        if (type.equalsIgnoreCase("union")) {
            this.table = ins.get(0).getTable();
            for (int i = 1; i < ins.size(); i++) {
                this.table = this.table.union(ins.get(i).getTable());
            }
        } else if (type.equalsIgnoreCase("unionAll")) {
            this.table = ins.get(0).getTable();
            for (int i = 1; i < ins.size(); i++) {
                this.table = this.table.unionAll(ins.get(i).getTable());
            }
        } else {
            throw new RuntimeException("Not supported union type: " + type);
        }
        return this;
    }
}
