package com.alibaba.alink.batchoperator.dataproc;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.RowTypeDataSet;

import java.util.List;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;


public class CrossBatchOp extends BatchOperator {

    public enum Type {

        Auto,
        WithTiny,
        WithHuge
    }
    private Type type = Type.Auto;

    public CrossBatchOp() {
        super(null);
    }

    public CrossBatchOp(CrossBatchOp.Type type) {
        super(null);
        this.type = type;
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        if (null == ins || ins.size() != 2) {
            throw new RuntimeException("Need 2 inputs!");
        }
        Table table1 = ins.get(0).getTable();
        Table table2 = ins.get(1).getTable();
        DataSet<Row> r;
        if (Type.WithTiny == type) {
            r = RowTypeDataSet.fromTable(table1).crossWithTiny(RowTypeDataSet.fromTable(table2)).with(new MyCrossFunc());
        } else if (Type.WithHuge == type) {
            r = RowTypeDataSet.fromTable(table1).crossWithHuge(RowTypeDataSet.fromTable(table2)).with(new MyCrossFunc());
        } else {
            r = RowTypeDataSet.fromTable(table1).cross(RowTypeDataSet.fromTable(table2)).with(new MyCrossFunc());
        }
        this.table = RowTypeDataSet.toTable(r,
                ArrayUtil.arrayMerge(table1.getSchema().getColumnNames(), table2.getSchema().getColumnNames()),
                ArrayUtil.arrayMerge(table1.getSchema().getTypes(), table2.getSchema().getTypes()));
        return this;
    }

    private static class MyCrossFunc implements CrossFunction<Row, Row, Row> {

        @Override
        public Row cross(Row in1, Row in2) throws Exception {
            int n1 = in1.getArity();
            int n2 = in2.getArity();
            Row r = new Row(n1 + n2);
            for (int i = 0; i < n1; i++) {
                r.setField(i, in1.getField(i));
            }
            for (int i = 0; i < n2; i++) {
                r.setField(i + n1, in2.getField(i));
            }
            return r;
        }
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new RuntimeException("Need 2 inputs!");
    }

}
