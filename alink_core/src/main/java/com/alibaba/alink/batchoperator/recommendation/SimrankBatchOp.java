package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.recommendation.Simrank;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * parameters:
 * -# userColName: required
 * -# itemColName: required
 * -# weightColName: optional
 * -# predResultColName: required
 * -# modelType: optional, "simrank" or "simrank++", default "simrank++"
 * -# numWalks: optional, default 100
 * -# walkLength: optional, default 10
 * -# topK: optional, default 100
 * -# decayFactor: optional, default 0.8
 *
 */
public class SimrankBatchOp extends BatchOperator {

    /**
     * Empty constructor.
     */
    public SimrankBatchOp() {
        super(new AlinkParameter());
    }

    /**
     * Constructor
     *
     * @param params
     */
    public SimrankBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * Set userColName
     *
     * @param value
     */
    public SimrankBatchOp setUserColName(String value) {
        putParamValue("userColName", value);
        return this;
    }

    /**
     * Set weightColName
     *
     * @param value
     */
    public SimrankBatchOp setWeightColName(String value) {
        putParamValue("weightColName", value);
        return this;
    }

    /**
     * Set itemColName
     *
     * @param value
     */
    public SimrankBatchOp setItemColName(String value) {
        putParamValue("itemColName", value);
        return this;
    }

    /**
     * Set predResultColName
     *
     * @param value
     */
    public SimrankBatchOp setPredResultColName(String value) {
        putParamValue(ParamName.predResultColName, value);
        return this;
    }

    /**
     * Set numWalks
     *
     * @param value
     * @return
     */
    public SimrankBatchOp setNumWalks(int value) {
        putParamValue("numWalks", value);
        return this;
    }

    /**
     * Set walkLength
     *
     * @param value
     * @return
     */
    public SimrankBatchOp setWalkLength(int value) {
        putParamValue("walkLength", value);
        return this;
    }

    /**
     * Set topK
     *
     * @param value
     * @return
     */
    public SimrankBatchOp setTopK(int value) {
        putParamValue("topK", value);
        return this;
    }

    /**
     * Link from upstream operators.
     *
     * @param ins: Upsream operators.
     * @return this operator
     */
    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }

    /**
     * Link from upstream operator.
     *
     * @param in: Upsream operator
     * @return this operator
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        String userColName = params.getString("userColName");
        String itemColName = params.getString("itemColName");
        String weightColName = params.getStringOrDefault("weightColName", null);
        if (null != weightColName && weightColName.trim().isEmpty()) {
            weightColName = null;
        }
        String predResultColName = params.getString(ParamName.predResultColName);

        final int userColIdx = TableUtil.findIndexFromName(in.getColNames(), userColName);
        final int itemColIdx = TableUtil.findIndexFromName(in.getColNames(), itemColName);
        final int weightColIdx = (weightColName == null) ? -1 : TableUtil.findIndexFromName(in.getColNames(), weightColName);

        if (userColIdx < 0) {
            throw new RuntimeException("SimrankBatchOp: can't find userColName " + userColName);
        }
        if (itemColIdx < 0) {
            throw new RuntimeException("SimrankBatchOp: can't find itemColName " + itemColName);
        }
        if (weightColName != null && weightColIdx < 0) {
            throw new RuntimeException("SimrankBatchOp: can't find weightColName " + weightColName);
        }

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
            Simrank simrank = new Simrank(this.params);

            DataSet<Tuple2<Integer, String>> result = simrank.batchPredict(simrankInput);

            DataSet<Row> output = result
                    .map(new MapFunction<Tuple2<Integer, String>, Row>() {
                        @Override
                        public Row map(Tuple2<Integer, String> value) throws Exception {
                            return Row.of((long) value.f0, value.f1);
                        }
                    });

            TableSchema schema = new TableSchema(
                    new String[]{itemColName, predResultColName},
                    new TypeInformation<?>[]{Types.LONG(), Types.STRING()}
            );

            this.table = RowTypeDataSet.toTable(output, schema);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        return this;
    }
}

