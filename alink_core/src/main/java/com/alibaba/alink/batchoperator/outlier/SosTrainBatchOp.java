package com.alibaba.alink.batchoperator.outlier;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.outlier.SOSModel;
import com.alibaba.alink.common.outlier.SOS;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Stochastic Outlier Selection algorithm.
 * <p>
 * input parameters:
 * -# featureColNames: optional, default null (all numeric columns will be selected)
 *    the value type can be any numerical types, all will be converted to double
 * -# perplexity: optional, default 4.0
 *
 */
public class SosTrainBatchOp extends BatchOperator {

    private String[] featureColNames = null;

    public SosTrainBatchOp(AlinkParameter params) {
        super(params);
        this.featureColNames = params.getStringArrayOrDefault(ParamName.featureColNames, null);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        if (null == this.featureColNames) {
            this.featureColNames = TableUtil.getNumericColNames(in.getSchema());
        }

        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < featureColNames.length; i++) {
            if (i > 0) {
                sbd.append(", ");
            }
            sbd.append("cast(" )
                .append(featureColNames[i])
                .append(" as DOUBLE) as ")
                .append(featureColNames[i]);
            //sbd.append(featureColNames[i]).append(".cast(DOUBLE)");
        }
        BatchOperator dataOp = in.select(sbd.toString());

        try {
            // tuple2: id, coords
            DataSet<Tuple2<Integer, DenseVector>> sosInput = DataSetUtils
                    .zipWithIndex(dataOp.getDataSet())
                    .map(new MapFunction<Tuple2<Long, Row>, Tuple2<Integer, DenseVector>>() {
                        @Override
                        public Tuple2<Integer, DenseVector> map(Tuple2<Long, Row> in) throws Exception {
                            DenseVector coord = new DenseVector(in.f1.getArity());
                            for (int i = 0; i < in.f1.getArity(); i++) {
                                coord.set(i, (Double) (in.f1.getField(i)));
                            }
                            return new Tuple2<>(in.f0.intValue(), coord);
                        }
                    });

            SOS sos = new SOS(this.params);

            // tuple3: id, beta, sumA
            DataSet<Tuple3<Integer, Double, Double>> beta = sos.train(sosInput);

            // tuple3: coord, beta, sumA
            DataSet<Tuple3<DenseVector, Double, Double>> modelDataRows = beta
                    .join(sosInput)
                    .where(0)
                    .equalTo(0)
                    .projectSecond(1).projectFirst(1, 2);

            final String[] featureNames = this.featureColNames.clone();

            DataSet<Row> modelRows = modelDataRows
                    .mapPartition(new MapPartitionFunction<Tuple3<DenseVector, Double, Double>, Row>() {
                        @Override
                        public void mapPartition(Iterable<Tuple3<DenseVector, Double, Double>> values, Collector<Row> out) throws Exception {

                            List<DenseVector> features = new ArrayList<>();
                            List<Double> beta = new ArrayList<>();
                            List<Double> sumA = new ArrayList<>();

                            Iterator<Tuple3<DenseVector, Double, Double>> samples = values.iterator();

                            while (samples.hasNext()) {
                                Tuple3<DenseVector, Double, Double> sample = samples.next();
                                features.add(sample.f0);
                                beta.add(sample.f1);
                                sumA.add(sample.f2);
                            }

                            SOSModel model = new SOSModel(features.toArray(new DenseVector[features.size()]),
                                    beta.toArray(new Double[beta.size()]), sumA.toArray(new Double[sumA.size()]));

                            model.getMeta().put("featureColNames", featureNames);
                            model.getMeta().put("numTrainingSamples", features.size());
                            model.getMeta().put("numFeatures", featureNames.length);

                            List<Row> rows = model.save(); // meta plus data
                            for (Row row : rows) {
                                out.collect(row);
                            }
                        }
                    })
                    .setParallelism(1);

            this.table = RowTypeDataSet.toTable(modelRows, AlinkModel.DEFAULT_MODEL_SCHEMA);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }

        return this;
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins) {
        return linkFrom(ins.get(0));
    }
}
