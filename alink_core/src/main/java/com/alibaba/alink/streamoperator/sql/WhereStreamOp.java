package com.alibaba.alink.streamoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class WhereStreamOp extends TableApiStreamOp {

    public WhereStreamOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "where");
    }

    public WhereStreamOp(String param) {
        this(new AlinkParameter().put("param", param));
    }
}
