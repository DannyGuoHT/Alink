package com.alibaba.alink.streamoperator.source;

import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;


public class TableSourceStreamOp extends StreamOperator {

    public TableSourceStreamOp(DataStream<Row> rows, String[] colNames, TypeInformation<?>[] colTypes) {
        this(RowTypeDataStream.toTable(rows, colNames, colTypes));
    }

    public TableSourceStreamOp(Table table) {
        super(null);
        if (null == table) {
            throw new RuntimeException();
        }
        this.table = table;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
