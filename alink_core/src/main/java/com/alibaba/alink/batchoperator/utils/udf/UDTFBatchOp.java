package com.alibaba.alink.batchoperator.utils.udf;

import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


class UDTFBatchOp extends BatchOperator {

    private final TableFunction <Row> tf;
    private final String selectedColName;
    private final String[] outputColNames;
    private final String[] keepColNames;

    public UDTFBatchOp(String selectedColName, String newColName, TableFunction <Row> tf) {
        this(selectedColName, new String[]{newColName}, tf);
    }

    public UDTFBatchOp(String selectedColName, String newColName, TableFunction <Row> tf, String[] keepColNames) {
        this(selectedColName, new String[]{newColName}, tf, keepColNames);
    }

    public UDTFBatchOp(String selectedColName, String[] outputColNames, TableFunction <Row> tf) {
        this(selectedColName, outputColNames, tf, null);
    }

    public UDTFBatchOp(String selectedColName, String[] outputColNames, TableFunction <Row> tf, String[] keepColNames) {
        super(null);
        if (TableUtil.findIndexFromName(outputColNames, selectedColName) >= 0) {
            throw new RuntimeException("The output colNames can not contain input colName!");
        }
        this.outputColNames = outputColNames;
        this.selectedColName = selectedColName;
        this.tf = tf;
        this.keepColNames = keepColNames;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        this.table = exec(in, this.selectedColName, this.outputColNames, this.tf, this.keepColNames);
        return this;
    }

    static Table exec(BatchOperator in, String selectColName, String[] newColNames, TableFunction <Row> tf, String[] keepOldColNames) {
        String fname = "f" + Long.toString(System.currentTimeMillis());
        AlinkSession.getBatchTableEnvironment().registerFunction(fname, tf);
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

        return AlinkSession.getBatchTableEnvironment().sql(sqlClause);
    }

}
