package com.alibaba.alink.streamoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class SelectStreamOp extends TableApiStreamOp {
    public SelectStreamOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "select");
    }

    public SelectStreamOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

}
