package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class RightOuterJoinBatchOp extends TableApiBatchOp {

    public RightOuterJoinBatchOp(String joinPredicate, String selectClause) {
        this(new AlinkParameter().put("whereClause", joinPredicate).putIgnoreNull("selectClause", selectClause));
    }

    public RightOuterJoinBatchOp(String joinPredicate) {
        this(joinPredicate, null);
    }

    public RightOuterJoinBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "rightOuterJoin");
    }
}
