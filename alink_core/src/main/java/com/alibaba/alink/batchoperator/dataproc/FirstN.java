package com.alibaba.alink.batchoperator.dataproc;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

public class FirstN extends BatchOperator {

    private final int n;

    public FirstN(int n) {
        super(null);
        this.n = n;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        DataSet<Row> ds = RowTypeDataSet.fromTable(in.getTable()).first(n);
        this.table = RowTypeDataSet.toTable(ds, in.getSchema());
        return this;
    }

}
