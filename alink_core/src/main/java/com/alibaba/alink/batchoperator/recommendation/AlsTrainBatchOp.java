package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.recommendation.ALS;
import com.alibaba.alink.common.recommendation.ALSTopKModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

import static com.alibaba.alink.common.AlinkSession.gson;

/**
 * input parameters:
 * -# userColName: required, corresponding column should be integer/long type, need not be consecutive
 * -# itemColName: required, corresponding column should be integer/long type, need not be consecutive
 * -# rateColName: required, corresponding column should be numeric type
 * -# task: optional, "rating" or "topk", default "topk"
 * -# numFactors: optional, default 10
 * -# numIter: optional, default 10
 * -# topK: optional, default 100
 * -# lambda: optional, default 0.1, the regularize parameter
 * -# implicitPref: optional, default false
 * If true, implicit preferences are handled, see reference 2.
 * In a nutshell, the input rating is not a score given from a user to an item,
 * but 'confidence' that a user 'likes' an item. It can be, for examples,
 * the elapsed time a user spends on an item.
 * -# alpha: optional, default 40.0
 * <p>
 * reference:
 * 1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007
 * 2. implicit feedback: Collaborative Filtering for Implicit Feedback Datasets, 2008
 *
 */
public class AlsTrainBatchOp extends BatchOperator {
    private String modelName = null;
    private String userColName = null;
    private String itemColName = null;
    private String rateColName = null;
    private String task = null; // "topK" | "rating"

    public AlsTrainBatchOp(AlinkParameter params) {
        super(params);
        initWithParams(params);
    }

    private void initWithParams(AlinkParameter params) {
        this.userColName = params.getString("userColName");
        this.itemColName = params.getString("itemColName");
        this.rateColName = params.getString("rateColName");
        this.modelName = params.getStringOrDefault("modelName", "ALSModel");
        this.task = params.getStringOrDefault("task", "topK").toLowerCase();
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        final int userColIdx = TableUtil.findIndexFromName(in.getColNames(), userColName);
        final int itemColIdx = TableUtil.findIndexFromName(in.getColNames(), itemColName);
        final int rateColIdx = TableUtil.findIndexFromName(in.getColNames(), rateColName);

        if (userColIdx < 0)
            throw new RuntimeException("ALSTrainBatchOp: can't find useColName " + userColName);
        if (itemColIdx < 0)
            throw new RuntimeException("ALSTrainBatchOp: can't find itemColName " + itemColName);
        if (rateColIdx < 0)
            throw new RuntimeException("ALSTrainBatchOp: can't find rateColName " + rateColName);


        if (this.task.equals("topk")) {
            try {
                // tuple3: userId, itemId, rating
                DataSet<Tuple3<Integer, Integer, Float>> alsInput = in.getDataSet()
                        .map(new MapFunction<Row, Tuple3<Integer, Integer, Float>>() {
                            @Override
                            public Tuple3<Integer, Integer, Float> map(Row value) throws Exception {
                                return new Tuple3<>(
                                        ((Number) value.getField(userColIdx)).intValue(),
                                        ((Number) value.getField(itemColIdx)).intValue(),
                                        ((Number) value.getField(rateColIdx)).floatValue());
                            }
                        });

                ALS als = new ALS(super.getParams());
                DataSet<Tuple3<Integer, List<Integer>, List<Float>>> rec = als.recommend(alsInput, true);

                final String colDelimiter = ",";
                final String valDelimiter = ":";
                final String userColName = this.userColName;
                final String itemColName = this.itemColName;

                DataSet<Row> output = rec.map(new MapFunction<Tuple3<Integer, List<Integer>, List<Float>>, Row>() {
                    @Override
                    public Row map(Tuple3<Integer, List<Integer>, List<Float>> value) throws Exception {
                        StringBuilder sbd = new StringBuilder();
                        int n = value.f2.size();

                        for (int i = 0; i < n; i++) {
                            if (i > 0) {
                                sbd.append(colDelimiter);
                            }
                            sbd.append(value.f1.get(i)).append(valDelimiter).append(value.f2.get(i));
                        }

                        return Row.of("user", value.f0, sbd.toString());
                    }
                });

                DataSet<Row> modelRows = output
                        .mapPartition(new RichMapPartitionFunction<Row, Row>() {
                            @Override
                            public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
                                int taskId = getRuntimeContext().getIndexOfThisSubtask();
                                ALSTopKModel model = new ALSTopKModel(taskId, values.iterator());
                                model.getMeta().put("userColName", userColName);
                                model.getMeta().put("itemColName", itemColName);
                                model.getMeta().put("task", "topK");

                                List<Row> rows = model.save(); // meta plus data
                                for (Row row : rows) {
                                    out.collect(row);
                                }
                            }
                        });

                this.table = RowTypeDataSet.toTable(modelRows, AlinkModel.DEFAULT_MODEL_SCHEMA);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } else if (this.task.equals("rating")) {
            try {
                // tuple3: userId, itemId, rating
                DataSet<Tuple3<Integer, Integer, Float>> alsInput = in.getDataSet()
                        .map(new MapFunction<Row, Tuple3<Integer, Integer, Float>>() {
                            @Override
                            public Tuple3<Integer, Integer, Float> map(Row value) throws Exception {
                                return new Tuple3<>(
                                        ((Number) value.getField(userColIdx)).intValue(),
                                        ((Number) value.getField(itemColIdx)).intValue(),
                                        ((Number) value.getField(rateColIdx)).floatValue());
                            }
                        });

                ALS als = new ALS(super.getParams());
                als.fit(alsInput);
                DataSet<Tuple3<Byte, Long, float[]>> factors = als.model.getFactors();

                final String userColName = this.userColName;
                final String itemColName = this.itemColName;

                DataSet<Row> output = factors.mapPartition(new RichMapPartitionFunction<Tuple3<Byte, Long, float[]>, Row>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Byte, Long, float[]>> values, Collector<Row> out) throws Exception {
                        int taskId = getRuntimeContext().getIndexOfThisSubtask();
                        if (taskId == 0) {
                            AlinkParameter meta = new AlinkParameter();
                            meta.put("userColName", userColName);
                            meta.put("itemColName", itemColName);
                            meta.put("task", "rating");
                            out.collect(Row.of(0L, meta.toJson(), "null", "null"));
                        }

                        for (Tuple3<Byte, Long, float[]> v : values) {
                            out.collect(Row.of(1L, String.valueOf(v.f0), String.valueOf(v.f1), gson.toJson(v.f2)));
                        }
                    }
                });

                this.table = RowTypeDataSet.toTable(output, AlinkModel.DEFAULT_MODEL_SCHEMA);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("unknown task type " + this.task);
        }

        return this;
    }
}
