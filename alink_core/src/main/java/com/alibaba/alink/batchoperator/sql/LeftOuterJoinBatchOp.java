package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class LeftOuterJoinBatchOp extends TableApiBatchOp {

    public LeftOuterJoinBatchOp(String joinPredicate, String selectClause) {
        this(new AlinkParameter().put("whereClause", joinPredicate).putIgnoreNull("selectClause", selectClause));
    }

    public LeftOuterJoinBatchOp(String joinPredicate) {
        this(joinPredicate, null);
    }

    public LeftOuterJoinBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "leftOuterJoin");
    }
}
