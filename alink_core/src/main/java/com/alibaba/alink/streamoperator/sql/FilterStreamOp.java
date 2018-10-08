package com.alibaba.alink.streamoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class FilterStreamOp extends TableApiStreamOp {

    public FilterStreamOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "filter");
    }

    public FilterStreamOp(String param) {
        this(new AlinkParameter().put("filter", param));
    }
}
