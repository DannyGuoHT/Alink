package com.alibaba.alink.batchoperator.dataproc;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.SampleWithFraction;
import org.apache.flink.types.Row;


public class SampleBatchOp extends BatchOperator {

    private double ratio;
    private boolean withReplacement;
    private Long seed;


    public SampleBatchOp(AlinkParameter params) {
        super(params);
        this.ratio = params.getDouble("ratio");
        this.withReplacement = params.getBoolOrDefault("withReplacement", false);
        this.seed = null;
    }

    public SampleBatchOp(double ratio) {
        super(null);
        this.ratio = ratio;
        this.withReplacement = false;
        this.seed = null;
    }

    public SampleBatchOp(double ratio, boolean withReplacement) {
        super(null);
        this.ratio = ratio;
        this.withReplacement = withReplacement;
        this.seed = null;
    }

    public SampleBatchOp(double ratio, boolean withReplacement, long seed) {
        super(null);
        this.ratio = ratio;
        this.withReplacement = withReplacement;
        this.seed = seed;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        long randseed;
        if (null == this.seed) {
            randseed = Utils.RNG.nextLong();
        } else {
            randseed = this.seed;
        }

        DataSet<Row> rst = RowTypeDataSet.fromTable(in.getTable())
                .mapPartition(new SampleWithFraction(this.withReplacement, this.ratio, randseed));

        this.table = RowTypeDataSet.toTable(rst, in.getSchema());
        return this;
    }

}
