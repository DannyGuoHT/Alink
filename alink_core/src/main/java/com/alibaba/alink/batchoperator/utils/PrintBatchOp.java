package com.alibaba.alink.batchoperator.utils;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.RowTypeDataSet;

import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;


public class PrintBatchOp extends BatchOperator {

    private static PrintStream batchPrintStream = System.err;

    public static void setBatchPrintStream(PrintStream printStream) {
        batchPrintStream = printStream;
    }

    public PrintBatchOp() {
        super(null);
    }

    public PrintBatchOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        this.table = in.getTable();
        if (null != this.table) {
            try {
                batchPrintStream.println(ArrayUtil.array2str(this.getColNames(), ", "));
                List <Row> elements = RowTypeDataSet.fromTable(this.table).collect();
                Iterator var2 = elements.iterator();
                while (var2.hasNext()) {
                    Row e = (Row) var2.next();
                    batchPrintStream.println(e);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return this;
    }

}
