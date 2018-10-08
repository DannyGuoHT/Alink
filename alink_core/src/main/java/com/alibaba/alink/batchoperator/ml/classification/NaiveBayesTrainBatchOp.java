package com.alibaba.alink.batchoperator.ml.classification;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.*;
import com.alibaba.alink.common.ml.MLModel;
import com.alibaba.alink.common.ml.MLModelType;
import com.alibaba.alink.common.ml.NaiveBayesModel;
import com.alibaba.alink.common.ml.NaiveBayesProbInfo;
import com.alibaba.alink.io.utils.JdbcTypeConverter;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * *
 *
 *
 * Naive Bayes Classifiers.
 *
 * we support the multinomial Naive Bayes and multinomial NB model, a probabilistic learning method.
 * here, feature values of train table must be nonnegative.
 *
 * details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */

public class NaiveBayesTrainBatchOp extends BatchOperator {

    public enum BayesType {
        /* Multinomial */
        Multinomial,
        /* Bernoulli */
        Bernoulli
    }

    /**
     * constructor.
     */
    public NaiveBayesTrainBatchOp() {
        super(null);
    }

    /**
     * constructor.
     *
     * @param params the parameters of the algorithm.
     */
    public NaiveBayesTrainBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * constructor.
     *
     * @param tensorColName tensor column names of input table.
     * @param lableColName  label column names of input table.
     * @param bayesType     type of method. Supported options: Multinomial and Bernoulli.
     * @param weightColName weight column names of input table.
     * @param smoothing     The smoothing parameter.
     * @param isSparse      the data is sparse or not.
     */
    public NaiveBayesTrainBatchOp(String tensorColName, String lableColName,
                                  String bayesType, String weightColName, double smoothing, boolean isSparse) {
        this(new AlinkParameter()
            .put(ParamName.tensorColName, tensorColName)
            .put(ParamName.labelColName, lableColName)
            .put("bayesType", bayesType)
            .put(ParamName.weightColName, weightColName)
            .put("smoothing", smoothing)
            .put(ParamName.isSparse, isSparse)
        );
    }

    /**
     * constructor.
     *
     * @param featureColNames tensor column names of input table.
     * @param lableColName    label column names of input table.
     * @param bayesType       type of method. Supported options: Multinomial and Bernoulli.
     * @param weightColName   weight column names of input table.
     * @param smoothing       The smoothing parameter.
     */
    public NaiveBayesTrainBatchOp(String[] featureColNames, String lableColName,
                                  String bayesType, String weightColName, double smoothing) {
        this(
        );
    }

    /**
     * set feature column name
     *
     * @param value feature column name
     * @return this
     */
    public NaiveBayesTrainBatchOp setFeatureColNames(String[] value) {
        putParamValue(ParamName.featureColNames, value);
        return this;
    }

    /**
     * set label column name
     *
     * @param value label column name
     * @return this
     */
    public NaiveBayesTrainBatchOp setLabelColName(String value) {
        putParamValue(ParamName.labelColName, value);
        return this;
    }

    /**
     * set weight column name
     *
     * @param value weight column name
     * @return this
     */
    public NaiveBayesTrainBatchOp setWeightColName(String value) {
        putParamValue(ParamName.weightColName, value);
        return this;
    }

    /**
     * set tensor column name
     *
     * @param value tensor column name
     * @return this
     */
    public NaiveBayesTrainBatchOp setTensorColName(String value) {
        putParamValue(ParamName.tensorColName, value);
        return this;
    }

    /**
     * set bayes type
     *
     * @param value bayes type : "Multinomial" or "Bernoulli"
     * @return this
     */
    public NaiveBayesTrainBatchOp setBayesType(String value) {
        putParamValue("bayesType", value);
        return this;
    }

    /**
     * set isSparse.
     *
     * @param value isSparse.
     * @return this
     */
    public NaiveBayesTrainBatchOp setIsSparse(boolean value) {
        putParamValue(ParamName.isSparse, value);
        return this;
    }

    /**
     * set smoothing
     *
     * @param value smoothing
     * @return this
     */
    public NaiveBayesTrainBatchOp setSmoothing(double value) {
        putParamValue("smoothing", value);
        return this;
    }

    /**
     * the linkFrom function : train data and get a model.
     *
     * @param in input operator, which contains the data of train.
     * @return the model of bayes.
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        TypeInformation<?> labelType = null;
        String[] featureColNames = params.getStringArrayOrDefault(ParamName.featureColNames, null);
        String labelColName = params.getString(ParamName.labelColName);
        BayesType bayesType = BayesType.valueOf(params.getStringOrDefault("bayesType", "Multinomial"));
        String weightColName = params.getStringOrDefault(ParamName.weightColName, null);
        double smoothing = params.getDoubleOrDefault("smoothing", 0.5);
        String tensorColName = params.getStringOrDefault(ParamName.tensorColName, null);
        boolean isSparse = params.getBoolOrDefault(ParamName.isSparse, false);

        String[] colNames = in.getColNames();
        Integer[] featIdx;
        int featureLength = 1;

        if (featureColNames != null) {
            featureLength = featureColNames.length;
            featIdx = new Integer[featureLength];
            for (int i = 0; i < featureLength; ++i) {
                featIdx[i] = TableUtil.findIndexFromName(colNames, featureColNames[i]);
            }
        } else if (tensorColName != null) {
            featIdx = new Integer[1];
            featIdx[0] = TableUtil.findIndexFromName(colNames, tensorColName);
        } else {
            throw new RuntimeException("feature col info error.");
        }
        Integer labelIdx = TableUtil.findIndexFromName(colNames, labelColName);

        Integer weightIdx = (weightColName == null) ? -1 : TableUtil.findIndexFromName(colNames, weightColName);
        if (null == labelType) {
            labelType = in.getColTypes()[TableUtil.findIndexFromName(in.getColNames(), labelColName)];
        }

        // create the model meta
        AlinkParameter meta = new AlinkParameter()
            .put(ParamName.modelName, "NaiveBayes")
            .put("bayesType", bayesType)
            .put(ParamName.featureColNames, featureColNames)
            .put(ParamName.isSparse, isSparse)
            .put(ParamName.tensorColName, tensorColName)
            .put(ParamName.labelType, JdbcTypeConverter.getSqlType(labelType), String.class)
            .put(ParamName.modelType, MLModelType.NaiveBayesModel);

        // data transfer to double and add two columns (labels weight)
        DataSet<Tuple3<Object, Double, Vector>> tmp = in.getDataSet()
            .map(new Transfer(labelIdx, weightIdx, featIdx, featureLength,
                isSparse, tensorColName));

        /**
         * calculate the Feature Dimension of sparse vector.
         * here, we use mapReduce OP to get max index of all vectors.
         */
        DataSet<Integer> sparseFeatureDim = null;
        if (tensorColName != null) {
            if (isSparse) {
                sparseFeatureDim
                    = tmp.map(new MapFunction<Tuple3<Object, Double, Vector>, Integer>() {
                    @Override
                    public Integer map(Tuple3<Object, Double, Vector> value) throws Exception {
                        return ((SparseVector)value.f2).getMaxIndex() + 1;
                    }
                }).reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1,
                                          Integer value2) throws Exception {
                        return Math.max(value1, value2);
                    }
                });
            } else {
                sparseFeatureDim
                    = tmp.map(new MapFunction<Tuple3<Object, Double, Vector>, Integer>() {
                    @Override
                    public Integer map(Tuple3<Object, Double, Vector> value) throws Exception {
                        return (value.f2).size();
                    }
                });
            }
        } else {
            sparseFeatureDim = AlinkSession.getExecutionEnvironment().fromElements(featureColNames.length);
        }

        DataSet<Row> probs;
        /**
         * when data is sparse format, we need to broadcast the featureDim.
         */
        if (isSparse) {
            probs = tmp.groupBy(new SelectLabel())
                .combineGroup(new SparseCombineItem())
                .withBroadcastSet(sparseFeatureDim, "featureDim")
                .groupBy(new SelectLabel())
                .reduceGroup(new SparseReduceItem())
                .withBroadcastSet(sparseFeatureDim, "featureDim")
                .mapPartition(new GenerateModel(smoothing, bayesType, meta))
                .withBroadcastSet(sparseFeatureDim, "featureDim")
                .setParallelism(1);
        } else {
            /**
             * calculate the dense matrix of prob.
             */
            probs = tmp
                .groupBy(new SelectLabel())
                .combineGroup(new CombineItem(featureLength))
                .groupBy(new SelectLabel())
                .reduceGroup(new ReduceItem(featureLength))
                .mapPartition(new GenerateModel(smoothing, bayesType, meta))
                .withBroadcastSet(sparseFeatureDim, "featureDim")
                .setParallelism(1);
        }
        /* save the model matrix */
        this.table = RowTypeDataSet.toTable(probs, MLModel.getModelSchemaWithType(labelType));
        return this;
    }

    /**
     * transfer the data format.
     */
    public class Transfer implements MapFunction<Row, Tuple3<Object, Double, Vector>> {
        private Integer[] featIdx;
        private Integer weightIdx;
        private Integer labelIdx;
        private Integer featureLength;
        private String tensorColName = null;
        private boolean isSparse;

        /**
         * transfer the data format.
         *
         * @param labelIdx      idx of label.
         * @param weightIdx     idx of weight.
         * @param featIndices   indices of feature.
         * @param featureLength feature length.
         * @param isSparse      sparse or not.
         * @param tensorColName name of tensor column.
         */
        public Transfer(Integer labelIdx, Integer weightIdx, Integer[] featIndices, Integer featureLength,
                        boolean isSparse, String tensorColName) {
            this.featIdx = featIndices;
            this.weightIdx = weightIdx;
            this.labelIdx = labelIdx;
            this.featureLength = featureLength;
            this.tensorColName = tensorColName;
            this.isSparse = isSparse;
        }

        /**
         * transfer data format
         *
         * @param in sample row.
         * @return train data format.
         */
        @Override
        public Tuple3<Object, Double, Vector> map(Row in) throws Exception {
            Vector feature;
            Object labelVal = in.getField(this.labelIdx);
            Double weightVal = (this.weightIdx == -1) ? 1.0 : Double.parseDouble(
                in.getField(this.weightIdx).toString());

            if (isSparse) {
                String str = in.getField(featIdx[0]).toString();
                feature = Tensor.parse(str).toSparseVector();
            } else if (tensorColName != null) {
                String str = in.getField(featIdx[0]).toString();
                feature = Tensor.parse(str).toDenseVector();
            } else {
                feature = new DenseVector(featureLength);
                for (int i = 0; i < this.featureLength; ++i) {
                    Double val = Double.parseDouble(in.getField(this.featIdx[i]).toString());
                    feature.set(i, val);
                }
            }
            return new Tuple3<>(labelVal, weightVal, feature);
        }
    }

    /**
     * generate model.
     */
    public static class GenerateModel extends AbstractRichFunction
        implements MapPartitionFunction<Tuple3<Object, Double, Vector>, Row> {
        private int numFeature;
        private double smoothing;
        private BayesType bayesType;
        private AlinkParameter meta;

        public GenerateModel(double smoothing, BayesType bayesType, AlinkParameter meta) {
            this.smoothing = smoothing;
            this.bayesType = bayesType;
            this.meta = meta;
        }

        @Override
        public void mapPartition(Iterable<Tuple3<Object, Double, Vector>> values, Collector<Row> collector)
            throws Exception {
            double numDocs = 0.0;

            ArrayList<Tuple3<Object, Double, Vector>> modelArray = new ArrayList<>();

            for (Tuple3<Object, Double, Vector> tup : values) {
                numDocs += tup.f1;
                modelArray.add(tup);
            }
            int numLabels = modelArray.size();
            double piLog = Math.log(numDocs + numLabels * this.smoothing);

            /**
             *  prepare data for model
             */
            DenseMatrix theta = new DenseMatrix(numLabels, numFeature);
            double[] piArray = new double[numLabels];
            Object[] labels = new Object[numLabels];
            for (int i = 0; i < numLabels; ++i) {
                DenseVector feature = (DenseVector)modelArray.get(i).f2;
                double numTerm = 0.0;
                for (int j = 0; j < feature.size(); ++j) {
                    numTerm += feature.get(j);
                }
                double thetaLog = 0.0;
                switch (this.bayesType) {
                    case Multinomial: {
                        thetaLog += Math.log(numTerm + this.numFeature * this.smoothing);
                        break;
                    }
                    case Bernoulli: {
                        thetaLog += Math.log(modelArray.get(i).f1 + 2.0 * this.smoothing);
                        break;
                    }
                    default: {
                        break;
                    }
                }

                labels[i] = modelArray.get(i).f0;
                piArray[i] = Math.log(modelArray.get(i).f1 + this.smoothing) - piLog;

                for (int j = 0; j < feature.size(); ++j) {
                    theta.set(i, j, Math.log(feature.get(j) + this.smoothing) - thetaLog);
                }
            }
            NaiveBayesProbInfo data = new NaiveBayesProbInfo();
            data.labels = labels;
            data.theta = theta;
            data.piArray = piArray;
            NaiveBayesModel model = new NaiveBayesModel(this.meta, data);

            List<Row> modelRows = model.save();
            for (Row row : modelRows) {
                collector.collect(row);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.numFeature = (Integer)getRuntimeContext()
                .getBroadcastVariable("featureDim").get(0);
        }
    }

    /**
     * select label as a key.
     */
    public class SelectLabel implements KeySelector<Tuple3<Object, Double, Vector>, String> {

        public SelectLabel() {

        }

        @Override
        public String getKey(Tuple3<Object, Double, Vector> t3) {
            return t3.f0.toString();
        }
    }

    /**
     * calc the sum of feature with same label.
     */
    public class CombineItem
        implements GroupCombineFunction<Tuple3<Object, Double, Vector>, Tuple3<Object, Double, Vector>> {

        private Integer featureLength;

        public CombineItem(int featureLength) {
            this.featureLength = featureLength;
        }

        @Override
        public void combine(Iterable<Tuple3<Object, Double, Vector>> rows,
                            Collector<Tuple3<Object, Double, Vector>> out) throws Exception {
            Object label = null;
            double weightSum = 0.0;
            Vector featureSum = new DenseVector(this.featureLength);

            for (Tuple3<Object, Double, Vector> row : rows) {
                label = row.f0;
                double w = row.f1;
                weightSum += w;
                for (int i = 0; i < this.featureLength; ++i) {
                    featureSum.set(i, featureSum.get(i) + row.f2.get(i) * w);
                }
            }
            Tuple3<Object, Double, Vector> t3 = new Tuple3<>(label, weightSum, featureSum);

            out.collect(t3);
        }
    }

    /**
     * calc the sum of feature with same label for sparse data.
     */
    public class SparseCombineItem extends AbstractRichFunction
        implements GroupCombineFunction<Tuple3<Object, Double, Vector>, Tuple3<Object, Double, Vector>> {
        private int sparseFeatureDim = 0;

        public SparseCombineItem() {
        }

        @Override
        public void combine(Iterable<Tuple3<Object, Double, Vector>> rows,
                            Collector<Tuple3<Object, Double, Vector>> out) throws Exception {
            Object label = null;
            double weightSum = 0.0;

            Vector featureSum = new DenseVector(this.sparseFeatureDim);
            for (Tuple3<Object, Double, Vector> row : rows) {
                label = row.f0;

                double w = row.f1;
                weightSum += w;
                ((SparseVector)row.f2).setSize(this.sparseFeatureDim);
                int[] idx = ((SparseVector)row.f2).getIndices();
                double[] val = ((SparseVector)row.f2).getValues();
                for (int i = 0; i < idx.length; ++i) {
                    featureSum.add(idx[i], val[i]);
                }
            }
            Tuple3<Object, Double, Vector> t3 = new Tuple3<>(label, weightSum, featureSum);

            out.collect(t3);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.sparseFeatureDim = (Integer)getRuntimeContext()
                .getBroadcastVariable("featureDim").get(0);
        }

    }

    public class ReduceItem
        implements GroupReduceFunction<Tuple3<Object, Double, Vector>, Tuple3<Object, Double, Vector>> {
        private int featureLength;

        public ReduceItem(int featureLength) {
            this.featureLength = featureLength;
        }

        @Override
        public void reduce(Iterable<Tuple3<Object, Double, Vector>> values,
                           Collector<Tuple3<Object, Double, Vector>> out) throws Exception {
            Object label = null;
            double weightSum = 0.0;

            Vector featureSum = new DenseVector(this.featureLength);
            for (Tuple3<Object, Double, Vector> row : values) {
                label = row.f0;
                weightSum += row.f1;

                for (int i = 0; i < this.featureLength; ++i) {
                    featureSum.set(i, featureSum.get(i) + row.f2.get(i));
                }
            }
            Tuple3<Object, Double, Vector> t3 = new Tuple3<>(label, weightSum, featureSum);

            out.collect(t3);
        }
    }

    public class SparseReduceItem extends AbstractRichFunction
        implements GroupReduceFunction<Tuple3<Object, Double, Vector>, Tuple3<Object, Double, Vector>> {
        private int sparseFeatureDim = 0;

        public SparseReduceItem() {
        }

        @Override
        public void reduce(Iterable<Tuple3<Object, Double, Vector>> values,
                           Collector<Tuple3<Object, Double, Vector>> out) throws Exception {
            Object label = null;
            double weightSum = 0.0;
            Vector featureSum = new DenseVector(this.sparseFeatureDim);

            for (Tuple3<Object, Double, Vector> row : values) {
                label = row.f0;
                weightSum += row.f1;

                for (int i = 0; i < this.sparseFeatureDim; ++i) {
                    featureSum.set(i, featureSum.get(i) + row.f2.get(i));
                }
            }
            Tuple3<Object, Double, Vector> t3 = new Tuple3<>(label, weightSum, featureSum);

            out.collect(t3);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.sparseFeatureDim = (Integer)getRuntimeContext()
                .getBroadcastVariable("featureDim").get(0);
        }

    }
}
