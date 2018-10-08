package com.alibaba.alink.batchoperator.sql;

import com.alibaba.alink.common.AlinkParameter;

public class JoinBatchOp extends TableApiBatchOp {

    public JoinBatchOp(String joinPredicate, String selectClause) {
        this(new AlinkParameter().put("whereClause", joinPredicate).putIgnoreNull("selectClause", selectClause));
    }

    public JoinBatchOp(String joinPredicate) {
        this(joinPredicate, null);
    }

    public JoinBatchOp(AlinkParameter param) {
        super(param);
        this.params.put("op", "join");
    }
}
