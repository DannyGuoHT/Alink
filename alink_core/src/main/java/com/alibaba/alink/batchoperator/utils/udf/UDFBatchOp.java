package com.alibaba.alink.batchoperator.utils.udf;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

/**
 * <p>
 * outputColNames CAN NOT have the same colName with keepColName except the selectedColName.
 */
public class UDFBatchOp extends BatchOperator {

    private final UserDefinedFunction udf;
    private String selectedColName;
    private final String[] outputColNames;
    private String[] keepColNames;

    public UDFBatchOp(String selectedColName, String outputColName, UserDefinedFunction udf) {
        this(selectedColName, new String[]{outputColName}, udf);
    }

    public UDFBatchOp(String selectedColName, String outputColName, UserDefinedFunction udf, String[] keepColNames) {
        this(selectedColName, new String[]{outputColName}, udf, keepColNames);
    }

    public UDFBatchOp(String selectedColName, String[] outputColNames, UserDefinedFunction udf) {
        this(selectedColName, outputColNames, udf, null);
    }

    public UDFBatchOp(String selectedColName, String[] outputColNames, UserDefinedFunction udf, String[] keepColNames) {
        super(null);
        if (null == selectedColName) {
            throw new RuntimeException("Must input selectedColName!");
        } else {
            this.selectedColName = selectedColName;
            if (null == outputColNames) {
                this.outputColNames = new String[]{this.selectedColName};
            } else {
                this.outputColNames = outputColNames;
            }
            this.udf = udf;
            this.keepColNames = keepColNames;
        }
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String[] inColNames = in.getColNames();
        int inputColIndex = TableUtil.findIndexFromName(inColNames, this.selectedColName);
        if (inputColIndex < 0) {
            throw new RuntimeException("Input data table NOT have the col:" + this.selectedColName);
        }

        if (null == this.keepColNames) {
            if (TableUtil.findIndexFromName(outputColNames, this.selectedColName) >= 0) {
                this.keepColNames = new String[inColNames.length - 1];
                for (int i = 0; i < inputColIndex; i++) {
                    this.keepColNames[i] = inColNames[i];
                }
                for (int i = inputColIndex + 1; i < inColNames.length; i++) {
                    this.keepColNames[i - 1] = inColNames[i];
                }
            } else {
                this.keepColNames = in.getColNames();
            }
        }

        boolean hasSameColName = false;
        for (String name : outputColNames) {
            if (TableUtil.findIndexFromName(keepColNames, name) >= 0) {
                hasSameColName = true;
                break;
            }
        }
        if (hasSameColName) {
            throw new RuntimeException("keepColNames has the same name with outputColNames.");
        }


        if (TableUtil.findIndexFromName(outputColNames, this.selectedColName) < 0) {
            // selectedColName NOT in the outputColNames
            if (this.udf instanceof TableFunction) {
                this.table = UDTFBatchOp.exec(in, selectedColName, outputColNames, (TableFunction <Row>) udf, this.keepColNames);
            } else if (this.udf instanceof ScalarFunction) {
                this.table = UDSFBatchOp.exec(in, selectedColName, outputColNames[0], (ScalarFunction) udf, this.keepColNames);
            } else {
                throw new RuntimeException("Not supported yet!");
            }
        } else {
            // selectedColName is in the outputColNames, then it can not in the keepColNames
            String clauseAS = ArrayUtil.array2str(ArrayUtil.arrayMerge(this.keepColNames, this.outputColNames), ",");
            String tempColName = this.selectedColName + "_alink" + Long.toString(System.currentTimeMillis());
            int idx = TableUtil.findIndexFromName(outputColNames, this.selectedColName);
            outputColNames[idx] = tempColName;
            if (this.udf instanceof TableFunction) {
                this.table = UDTFBatchOp.exec(in, selectedColName, outputColNames, (TableFunction <Row>) udf, this.keepColNames).as(clauseAS);
            } else if (this.udf instanceof ScalarFunction) {
                this.table = UDSFBatchOp.exec(in, selectedColName, outputColNames[0], (ScalarFunction) udf, this.keepColNames).as(clauseAS);
            } else {
                throw new RuntimeException("Not supported yet!");
            }
        }

        return this;
    }

}
