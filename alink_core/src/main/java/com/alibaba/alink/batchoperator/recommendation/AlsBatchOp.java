package com.alibaba.alink.batchoperator.recommendation;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.recommendation.ALS;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * input parameters:
 * -# userColName: required, corresponding column should be integer/long type, need not be consecutive
 * -# itemColName: required, corresponding column should be integer/long type, need not be consecutive
 * -# rateColName: required, corresponding column should be numeric type
 * -# predResultColName: required
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
public class AlsBatchOp extends BatchOperator {

    /**
     * Empty constructor.
     */
    public AlsBatchOp() {
        super(new AlinkParameter());
    }

    /**
     * Constructor.
     *
     * @param params: parameters.
     */
    public AlsBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * Set userColName
     *
     * @param value
     */
    public AlsBatchOp setUserColName(String value) {
        putParamValue("userColName", value);
        return this;
    }

    /**
     * Set rateColName
     *
     * @param value
     */
    public AlsBatchOp setRateColName(String value) {
        putParamValue("rateColName", value);
        return this;
    }

    /**
     * Set itemColName
     *
     * @param value
     */
    public AlsBatchOp setItemColName(String value) {
        putParamValue("itemColName", value);
        return this;
    }

    /**
     * Set predResultColName
     *
     * @param value
     */
    public AlsBatchOp setPredResultColName(String value) {
        putParamValue(ParamName.predResultColName, value);
        return this;
    }

    /**
     * Set numFactors
     * @param value
     * @return
     */
    public AlsBatchOp setNumFactors(int value) {
        putParamValue("numFactors", value);
        return this;
    }

    /**
     * Set numIter
     * @param value
     * @return
     */
    public AlsBatchOp setNumIter(int value) {
        putParamValue(ParamName.numIter, value);
        return this;
    }

    /**
     * Set topK
     * @param value
     * @return
     */
    public AlsBatchOp setTopK(int value) {
        putParamValue("topK", value);
        return this;
    }

    /**
     * Set lambda
     * @param value
     * @return
     */
    public AlsBatchOp setLambda(double value) {
        putParamValue("lambda", value);
        return this;
    }

    /**
     * Set implicitPref
     * @param value
     * @return
     */
    public AlsBatchOp setImplicitPref(boolean value) {
        putParamValue("implicitPref", value);
        return this;
    }

    /**
     * Set alpha
     * @param value
     * @return
     */
    public AlsBatchOp setAlpha(double value) {
        putParamValue("alpha", value);
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
        String rateColName = params.getString("rateColName");
        String predResultColName = params.getString(ParamName.predResultColName);

        final int userColIdx = TableUtil.findIndexFromName(in.getColNames(), userColName);
        final int itemColIdx = TableUtil.findIndexFromName(in.getColNames(), itemColName);
        final int rateColIdx = TableUtil.findIndexFromName(in.getColNames(), rateColName);

        if (userColIdx < 0 || itemColIdx < 0 || rateColIdx < 0) {
            throw new RuntimeException("can't find some of the columns");
        }

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

        try {
            ALS als = new ALS(super.getParams());
            DataSet<Tuple3<Integer, List<Integer>, List<Float>>> rec = als.recommend(alsInput, false);

            final String colDelimiter = ",";
            final String valDelimiter = ":";

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

                    return Row.of((long) value.f0, sbd.toString());
                }
            });

            TableSchema schema = new TableSchema(
                    new String[]{userColName, predResultColName},
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

