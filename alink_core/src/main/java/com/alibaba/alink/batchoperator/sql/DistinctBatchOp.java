package com.alibaba.alink.batchoperator.sql;

public class DistinctBatchOp extends TableApiBatchOp {

    public DistinctBatchOp() {
        super(null);
        this.params.put("op", "distinct");
    }

}
