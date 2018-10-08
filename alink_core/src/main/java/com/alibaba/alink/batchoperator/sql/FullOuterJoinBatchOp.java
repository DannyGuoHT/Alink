package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class FullOuterJoinBatchOp extends TableApiBatchOp {

    public FullOuterJoinBatchOp(String joinPredicate, String selectClause) {
        this(new AlinkParameter().put("whereClause", joinPredicate).putIgnoreNull("selectClause", selectClause));
    }

    public FullOuterJoinBatchOp(String joinPredicate) {
        this(joinPredicate, null);
    }

    public FullOuterJoinBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "fullOuterJoin");
    }

}
