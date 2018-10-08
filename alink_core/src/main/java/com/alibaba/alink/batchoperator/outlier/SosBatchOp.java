package com.alibaba.alink.batchoperator.outlier;

import java.util.List;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.outlier.SOS;
import com.alibaba.alink.common.utils.ArrayUtil;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.Row;

/**
 * Stochastic Outlier Selection algorithm.
 * <p>
 * input parameters:
 * -# featureColNames: optional, default null (all numeric columns will be selected)
 * the value type can be any numerical types, all will be converted to double
 * -# predResultColName: required
 * -# perplexity: optional, default 4.0
 *
 */
public class SosBatchOp extends BatchOperator {

    /**
     * Empty constructor.
     */
    public SosBatchOp() {
        super(new AlinkParameter());
    }

    /**
     * Constructor.
     *
     * @param params
     */
    public SosBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * Set selectedColNames
     *
     * @param value
     */
    public SosBatchOp setSelectedColNames(String[] value) {
        putParamValue(ParamName.selectedColNames, value);
        return this;
    }

    /**
     * Set selectedColNames
     *
     * @param value
     */
    public SosBatchOp setPredResultColName(String value) {
        putParamValue(ParamName.predResultColName, value);
        return this;
    }

    /**
     * Set perplexity
     *
     * @param value
     * @return
     */
    public SosBatchOp setPerplexity(double value) {
        putParamValue("perplexity", value);
        return this;
    }

    /**
     * Link from upstream operator.
     *
     * @param in: Upsream operator
     * @return this operator
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String[] selectedColNames = params.getStringArrayOrDefault(ParamName.selectedColNames, null);
        String predResultColName = params.getString(ParamName.predResultColName);

        if (null == selectedColNames) {
            selectedColNames = TableUtil.getNumericColNames(in.getSchema());
        }

        final int[] selectedColIndices = new int[selectedColNames.length];
        for (int i = 0; i < selectedColIndices.length; i++) {
            selectedColIndices[i] = TableUtil.findIndexFromName(in.getColNames(), selectedColNames[i]);
            if (selectedColIndices[i] < 0) {
                throw new IllegalArgumentException("can't find column " + selectedColNames[i]);
            }
        }

        try {
            DataSet <Tuple2 <Integer, Row>> pointsWithIndex = DataSetUtils
                .zipWithIndex(in.getDataSet())
                .map(new MapFunction <Tuple2 <Long, Row>, Tuple2 <Integer, Row>>() {
                    @Override
                    public Tuple2 <Integer, Row> map(Tuple2 <Long, Row> in) throws Exception {
                        return new Tuple2 <>(in.f0.intValue(), in.f1);
                    }
                });

            DataSet <Tuple2 <Integer, DenseVector>> sosInput = pointsWithIndex
                .map(new MapFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, DenseVector>>() {
                    @Override
                    public Tuple2 <Integer, DenseVector> map(Tuple2 <Integer, Row> in) throws Exception {
                        DenseVector coord = new DenseVector(selectedColIndices.length);
                        for (int i = 0; i < selectedColIndices.length; i++) {
                            coord.set(i, ((Number)in.f1.getField(selectedColIndices[i])).doubleValue());
                        }
                        return new Tuple2 <>(in.f0, coord);
                    }
                });

            SOS sos = new SOS(this.params);
            DataSet <Tuple2 <Integer, Double>> outlierProb = sos.run(sosInput);

            DataSet <Row> output = outlierProb
                .join(pointsWithIndex)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction <Tuple2 <Integer, Double>, Tuple2 <Integer, Row>, Row>() {
                    @Override
                    public Row join(Tuple2 <Integer, Double> in1, Tuple2 <Integer, Row> in2) throws Exception {
                        Row row = new Row(in2.f1.getArity() + 1);
                        for (int i = 0; i < in2.f1.getArity(); i++) {
                            row.setField(i, in2.f1.getField(i));
                        }
                        row.setField(in2.f1.getArity(), in1.f1);
                        return row;
                    }
                })
                .returns(new RowTypeInfo(ArrayUtil.arrayMerge(in.getColTypes(), Types.DOUBLE)));

            this.table = RowTypeDataSet.toTable(output, ArrayUtil.arrayMerge(in.getColNames(), predResultColName));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }

        return this;
    }

    /**
     * Link from upstream operators.
     *
     * @param ins: Upsream operators.
     * @return this operator
     */
    @Override
    public BatchOperator linkFrom(List <BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }
}
