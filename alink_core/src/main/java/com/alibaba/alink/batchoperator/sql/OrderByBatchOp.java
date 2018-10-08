package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class OrderByBatchOp extends TableApiBatchOp {
    public OrderByBatchOp(String param, int offset, int fetch) {
        this(new AlinkParameter().put("param", param).put("offset", offset).put("fetch", fetch));
    }

    public OrderByBatchOp(String param, int limit) {
        this(new AlinkParameter().put("param", param).put("limit", limit));
    }

    public OrderByBatchOp(String param) {
        this(new AlinkParameter().put("param", param));
    }

    public OrderByBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "orderBy");
    }

}
