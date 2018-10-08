package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class AsBatchOp extends TableApiBatchOp {

    public AsBatchOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

    public AsBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "as");
    }

}
