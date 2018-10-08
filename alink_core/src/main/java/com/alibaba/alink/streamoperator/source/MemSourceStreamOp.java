package com.alibaba.alink.streamoperator.source;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MemSourceStreamOp extends StreamOperator {

    public MemSourceStreamOp(String[] strs, String fieldName) {
        super(null);
        List <Row> rows = new ArrayList <Row>();
        for (String str : strs) {
            rows.add(Row.of(new Object[]{str}));
        }
        init(rows, new String[]{fieldName}, new TypeInformation<?>[]{TypeInformation.of(String.class)});
    }

    public MemSourceStreamOp(Double[] vals, String fieldName) {
        super(null);
        List <Row> rows = new ArrayList <Row>();
        for (Double val : vals) {
            rows.add(Row.of(new Object[]{val}));
        }
        init(rows, new String[]{fieldName}, new TypeInformation<?>[]{TypeInformation.of(Double.class)});
    }

    public MemSourceStreamOp(Object[][] objs, String[] fieldNames) {
        super(null);
        List <Row> rows = new ArrayList <Row>();
        for (int i = 0; i < objs.length; i++) {
            rows.add(Row.of(objs[i]));
        }
        init(rows, fieldNames);
    }

    public MemSourceStreamOp(List <Row> rows, TableSchema schema) {
        this(rows, schema.getColumnNames());
    }

    public MemSourceStreamOp(Row[] rows, String[] fieldNames) {
        this(Arrays.asList(rows), fieldNames);
    }

    public MemSourceStreamOp(List <Row> rows, String[] fieldNames) {
        super(null);
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

    private void init(List <Row> rows, String[] fieldNames, TypeInformation<?>[] colTypes) {
        if (null == fieldNames || fieldNames.length < 1) {
            throw new RuntimeException();
        }

        DataStream <Row> dastr = AlinkSession.getStreamExecutionEnvironment().fromCollection(rows);

        StringBuilder sbd = new StringBuilder();
        sbd.append(fieldNames[0]);
        for (int i = 1; i < fieldNames.length; i++) {
            sbd.append(",").append(fieldNames[i]);
        }

//        this.table = AlinkSession.getStreamTableEnvironment().fromDataStream(dastr, sbd.toString());
        this.table = RowTypeDataStream.toTable(dastr, fieldNames, colTypes);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
