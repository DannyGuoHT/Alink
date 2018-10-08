package com.alibaba.alink.streamoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class AsStreamOp extends TableApiStreamOp {

    public AsStreamOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "as");
    }

    public AsStreamOp(String param) {
        this(new AlinkParameter().put("param", param));
    }
}
