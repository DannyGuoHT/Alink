package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.recommendation.SimrankModel;
import com.alibaba.alink.common.recommendation.Simrank;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * input parameters:
 * -# userColName: required, corresponding column should be integer/long type, should be consecutive
 * -# itemColName: required, corresponding column should be integer/long type, should be consecutive
 * -# weightColName: optional, corresponding column should be numeric type
 * -# modelType: optional, "simrank" or "simrank++", default "simrank++"
 * -# numWalks: optional, default 100
 * -# walkLength: optional, default 10
 * -# topK: optional, default 100
 * -# decayFactor: optional, default 0.8
 *
 */
public class SimrankTrainBatchOp extends BatchOperator {
    private String userColName = null;
    private String itemColName = null;
    private String weightColName = null;

    public SimrankTrainBatchOp(AlinkParameter params) {
        super(params);
        initWithParams(params);
    }

    private void initWithParams(AlinkParameter params) {
        this.userColName = params.getString("userColName");
        this.itemColName = params.getString("itemColName");
        this.weightColName = params.getStringOrDefault("weightColName", null);
        if(null != this.weightColName && this.weightColName.trim().isEmpty())
            this.weightColName = null;
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        final int userColIdx = TableUtil.findIndexFromName(in.getColNames(), userColName);
        final int itemColIdx = TableUtil.findIndexFromName(in.getColNames(), itemColName);
        final int weightColIdx = (weightColName == null) ? -1 : TableUtil.findIndexFromName(in.getColNames(), weightColName);

        if (userColIdx < 0)
            throw new RuntimeException("SimrankTrainBatchOp: can't find useColName " + userColName);
        if (itemColIdx < 0)
            throw new RuntimeException("SimrankTrainBatchOp: can't find itemColName " + itemColName);
        if (weightColName != null && weightColIdx < 0)
            throw new RuntimeException("SimrankTrainBatchOp: can't find weightColName " + weightColName);

        // tuple3: userId, itemId, weight
        DataSet<Tuple3<Integer, Integer, Float>> simrankInput = null;

        if (weightColIdx >= 0) {
            simrankInput = in.getDataSet()
                    .map(new MapFunction<Row, Tuple3<Integer, Integer, Float>>() {
                        @Override
                        public Tuple3<Integer, Integer, Float> map(Row value) throws Exception {
                            return new Tuple3<>(
                                    ((Number) value.getField(userColIdx)).intValue(),
                                    ((Number) value.getField(itemColIdx)).intValue(),
                                    ((Number) value.getField(weightColIdx)).floatValue());
                        }
                    });
        } else {
            simrankInput = in.getDataSet()
                    .map(new MapFunction<Row, Tuple3<Integer, Integer, Float>>() {
                        @Override
                        public Tuple3<Integer, Integer, Float> map(Row value) throws Exception {
                            return new Tuple3<>(
                                    ((Number) value.getField(userColIdx)).intValue(),
                                    ((Number) value.getField(itemColIdx)).intValue(),
                                    1.0F);
                        }
                    });
        }

        try {
            Simrank simrank = new Simrank(this.getParams());
            final String userColName = this.userColName;
            final String itemColName = this.itemColName;

            DataSet<Tuple2<Integer, String>> result = simrank.batchPredict(simrankInput);

            DataSet<Row> modelRows = result
                    .mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Row>() {
                        @Override
                        public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Row> out) throws Exception {
                            SimrankModel model = new SimrankModel(values.iterator());
                            model.getMeta().put("userColName", userColName);
                            model.getMeta().put("itemColName", itemColName);

                            List<Row> rows = model.save(); // meta plus data
                            for (Row row : rows) {
                                out.collect(row);
                            }
                        }
                    })
                    .setParallelism(1);

            this.table = RowTypeDataSet.toTable(modelRows, AlinkModel.DEFAULT_MODEL_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        return this;
    }
}
