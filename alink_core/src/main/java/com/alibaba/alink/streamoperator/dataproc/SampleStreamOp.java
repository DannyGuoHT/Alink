package com.alibaba.alink.streamoperator.dataproc;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.streamoperator.StreamOperator;

import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class SampleStreamOp extends StreamOperator {

    private final double ratio;
    private final long maxSamples;

    public SampleStreamOp(double ratio) {
        super(null);
        this.ratio = ratio;
        this.maxSamples = Long.MAX_VALUE;
    }

    public SampleStreamOp(double ratio, long maxSamples) {
        super(null);
        this.ratio = ratio;
        this.maxSamples = maxSamples;
    }

    public SampleStreamOp(AlinkParameter params) {
        super(params);
        if (this.params.contains("sampleRate")) {
            this.ratio = this.params.getDouble("sampleRate");
            if (this.ratio <= 0 || this.ratio > 1) {
                throw new RuntimeException("sampleRate must be in (0, 1]!");
            }
        } else {
            this.ratio = this.params.getDouble("ratio");
            if (this.ratio <= 0 || this.ratio > 1) {
                throw new RuntimeException("ratio must be in (0, 1]!");
            }
        }
        this.maxSamples = Long.MAX_VALUE;
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        if (this.ratio >= 1) {
            this.table = in.getTable();
        } else {
            DataStream<Row> rst = RowTypeDataStream.fromTable(in.getTable())
                    .flatMap(new Sampler(this.ratio, this.maxSamples));

            this.table = RowTypeDataStream.toTable(rst, in.getSchema());
        }
        return this;
    }

    private static class Sampler implements FlatMapFunction<Row, Row> {

        private final double ratio;
        private final long maxSamples;
        private final Random rand;
        private long cnt = 0;

        public Sampler(double ratio, long maxSamples) {
            this.ratio = ratio;
            this.maxSamples = maxSamples;
            this.rand = new Random();
        }

        @Override
        public void flatMap(Row t, Collector<Row> clctr) throws Exception {
            if (cnt++ < this.maxSamples) {
                if (rand.nextDouble() <= this.ratio) {
                    clctr.collect(t);
                }
            }
        }
    }

}
