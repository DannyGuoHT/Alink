package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class SelectBatchOp extends TableApiBatchOp {

    public SelectBatchOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

    public SelectBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "select");
    }

}
