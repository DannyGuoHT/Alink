package com.alibaba.alink.batchoperator.dataproc;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.Row;


public class SampleWithSizeBatchOp extends BatchOperator {

    private final int numSample;
    private final boolean withReplacement;
    private final Long seed;

    public SampleWithSizeBatchOp(int numSamples) {
        super(null);
        this.numSample = numSamples;
        this.withReplacement = false;
        this.seed = null;
    }


    public SampleWithSizeBatchOp(int numSamples, boolean withReplacement) {
        super(null);
        this.numSample = numSamples;
        this.withReplacement = withReplacement;
        this.seed = null;
    }

    public SampleWithSizeBatchOp(int numSamples, boolean withReplacement, long seed) {
        super(null);
        this.numSample = numSamples;
        this.withReplacement = withReplacement;
        this.seed = seed;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        DataSet<Row> src = RowTypeDataSet.fromTable(in.getTable());
        DataSet<Row> rst;
        if (null == this.seed) {
            rst = DataSetUtils.sampleWithSize(src, withReplacement, numSample);
        } else {
            rst = DataSetUtils.sampleWithSize(src, withReplacement, numSample, seed);
        }
        this.table = RowTypeDataSet.toTable(rst, in.getSchema());
        return this;
    }

}
