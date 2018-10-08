package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class GroupByBatchOp extends TableApiBatchOp {

    public GroupByBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "groupBy");
    }

    public GroupByBatchOp(String groupByClause, String selectClause) {
        this(new AlinkParameter().put("selectClause", selectClause).put("groupByClause", groupByClause));
    }

}
