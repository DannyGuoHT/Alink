package com.alibaba.alink.streamoperator.utils.udf;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.streamoperator.StreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


class UDTFStreamOp extends StreamOperator {

    private final TableFunction <Row> tf;
    private final String inputColName;
    private final String[] newColNames;
    private final String[] keepOldColNames;

    public UDTFStreamOp(String inputColName, String newColName, TableFunction <Row> tf) {
        this(inputColName, new String[]{newColName}, tf);
    }

    public UDTFStreamOp(String inputColName, String newColName, TableFunction <Row> tf, String[] keepOldColNames) {
        this(inputColName, new String[]{newColName}, tf, keepOldColNames);
    }

    public UDTFStreamOp(String inputColName, String[] newColNames, TableFunction <Row> tf) {
        this(inputColName, newColNames, tf, null);
    }

    public UDTFStreamOp(String inputColName, String[] newColNames, TableFunction <Row> tf, String[] keepOldColNames) {
        super(null);
        if (TableUtil.findIndexFromName(newColNames, inputColName) >= 0) {
            throw new RuntimeException("The output colNames can not contain input colName!");
        }
        this.newColNames = newColNames;
        this.inputColName = inputColName;
        this.tf = tf;
        this.keepOldColNames = keepOldColNames;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        this.table = exec(in, this.inputColName, this.newColNames, this.tf, this.keepOldColNames);
        return this;
    }

    static Table exec(StreamOperator in, String selectColName, String[] newColNames, TableFunction <Row> tf, String[] keepOldColNames) {
        String fname = "f" + Long.toString(System.currentTimeMillis());
        AlinkSession.getStreamTableEnvironment().registerFunction(fname, tf);
        String[] colNames = keepOldColNames;
        if (null == colNames) {
            colNames = in.getColNames();
        }
        StringBuilder sbd;
        sbd = new StringBuilder();
        sbd.append(", LATERAL TABLE(").append(fname).append("(").append(selectColName).append(")) as T(").append(newColNames[0]);
        for (int i = 1; i < newColNames.length; i++) {
            sbd.append(", ").append(newColNames[i]);
        }
        sbd.append(")");
        String joinClause = sbd.toString();

        String selectClause = ArrayUtil.array2str(ArrayUtil.arrayMerge(colNames, newColNames), ",");

        String sqlClause = "SELECT " + selectClause + " FROM " + in.getTable() + joinClause;

        return AlinkSession.getStreamTableEnvironment().sql(sqlClause);
    }

}
