package com.alibaba.alink.batchoperator.source;

import com.alibaba.alink.batchoperator.BatchOperator;
import org.apache.flink.table.api.Table;


public class TableSourceBatchOp extends BatchOperator {

    public TableSourceBatchOp(Table table) {
        super(null);
        if (null == table) {
            throw new RuntimeException();
        }
        this.table = table;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
