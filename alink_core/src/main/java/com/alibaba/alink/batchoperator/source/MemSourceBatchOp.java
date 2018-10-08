package com.alibaba.alink.batchoperator.source;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;


public class MemSourceBatchOp extends BatchOperator {

    public MemSourceBatchOp(String[] strs, String fieldName) {
        super(null);
        List<Row> rows = new ArrayList<Row>();
        for (String str : strs) {
            rows.add(Row.of(new Object[]{str}));
        }
        init(rows, new String[]{fieldName}, new TypeInformation<?>[]{TypeInformation.of(String.class)});
    }

    public MemSourceBatchOp(Double[] vals, String fieldName) {
        super(null);
        List<Row> rows = new ArrayList<Row>();
        for (Double val : vals) {
            rows.add(Row.of(new Object[]{val}));
        }
        init(rows, new String[]{fieldName}, new TypeInformation<?>[]{TypeInformation.of(Double.class)});
    }

    public MemSourceBatchOp(List<Row> rows, TableSchema schema) {
        this(rows, schema.getColumnNames());
    }

    public MemSourceBatchOp(List<Row> rows, String[] fieldNames) {
        super(null);

        init(rows, fieldNames);
    }

    public MemSourceBatchOp(Object[][] objs, String[] fieldNames) {
        super(null);
        List<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < objs.length; i++) {
            rows.add(Row.of(objs[i]));
        }

        init(rows, fieldNames);
    }

    private void init(List<Row> rows, String[] fieldNames) {
        if (rows == null || rows.size() < 1) {
            throw new UnsupportedOperationException("container of row must be not empty.");
        }

        Row first = rows.iterator().next();

        int arity = first.getArity();

        TypeInformation<?>[] types = new TypeInformation[arity];

        for (int i = 0; i < arity; ++i) {
            types[i] = TypeExtractor.getForObject(first.getField(i));
        }

        init(rows, fieldNames, types);
    }

    private void init(List<Row> rows, String[] fieldNames, TypeInformation<?>[] colTypes) {
        if (null == fieldNames || fieldNames.length < 1) {
            throw new RuntimeException();
        }

        DataSet<Row> dastr = AlinkSession.getExecutionEnvironment().fromCollection(rows);

        this.table = RowTypeDataSet.toTable(dastr, fieldNames, colTypes);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
