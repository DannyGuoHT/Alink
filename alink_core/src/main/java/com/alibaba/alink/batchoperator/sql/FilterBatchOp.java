package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class FilterBatchOp extends TableApiBatchOp {

    public FilterBatchOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

    public FilterBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "filter");
    }
}
