package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class WhereBatchOp extends TableApiBatchOp {

    public WhereBatchOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

    public WhereBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "filter");
    }

}
