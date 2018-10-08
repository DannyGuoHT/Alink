package com.alibaba.alink.streamoperator.utils.udf;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;


class UDSFStreamOp extends StreamOperator {

    private final ScalarFunction sf;
    private final String selectedColName;
    private final String outputColName;
    private final String[] keepColNames;

    public UDSFStreamOp(String selectedColName, String outputColName, ScalarFunction sf) {
        this(selectedColName, outputColName, sf, null);
    }

    public UDSFStreamOp(String selectedColName, String outputColName, ScalarFunction sf, String[] keepColNames) {
        super(null);
        if (selectedColName.equals(outputColName)) {
            throw new RuntimeException("The input and output colName can not be same!");
        }
        this.outputColName = outputColName;
        this.selectedColName = selectedColName;
        this.sf = sf;
        this.keepColNames = keepColNames;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        this.table = exec(in, this.selectedColName, this.outputColName, this.sf, this.keepColNames);
        return this;
    }

    static Table exec(StreamOperator in, String selectColName, String newColName, ScalarFunction sf, String[] keepOldColNames) {
        String fname = "f" + Long.toString(System.currentTimeMillis());
        AlinkSession.getStreamTableEnvironment().registerFunction(fname, sf);
        String[] colNames = keepOldColNames;
        if (null == colNames) {
            colNames = in.getColNames();
        }
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < colNames.length; i++) {
            sbd.append(colNames[i]).append(", ");
        }
        sbd.append(fname).append("(").append(selectColName).append(") as ").append(newColName);

        return AlinkSession.getStreamTableEnvironment().sql("SELECT " + sbd.toString() + " FROM " + in.getTable());
    }

}
