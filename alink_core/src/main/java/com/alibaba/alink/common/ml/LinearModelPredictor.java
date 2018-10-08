package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.common.matrix.VectorOp;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.AlinkSession.gson;

public class LinearModelPredictor extends MLModelPredictor {

    private LinearModel model;
    private int[] featureIdx;
    private int featureN;
    private int out_size = -1;
    protected int tensorColIndex = -1;

    private int model_size;

    private DenseVector[] labelCodings;
    Map<Integer, ImmutablePair<Integer, Integer>> map; // multi Class using OvO needed

    public LinearModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        if (null != params) {
            String tensorColName = params.getStringOrDefault(ParamName.tensorColName, null);

            if (null != tensorColName && tensorColName.length() != 0) {
                this.tensorColIndex = TableUtil.findIndexFromName(dataSchema.getColumnNames(), tensorColName);
            }
        }
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        LinearModel linearModel = new LinearModel();
        linearModel.load(modelRows);
        loadModel(linearModel);

        if (model.isMultiClass && model.multiClassType.equals("OneVsOne")) {
            int nClass = model.labelValues.length;
            this.map = new HashMap<>();
            int iter = 0;
            for (int i = nClass - 1; i > 0; --i) {
                for (int j = 0; j < i; ++j) {
                    this.map.put(iter++, new ImmutablePair<Integer, Integer>(i, j));
                }
            }
        } else if (model.isMultiClass && model.multiClassType.equals("ManyVsMany")) {
            int nClass = model.labelValues.length;
            model_size = model.indicesForMxM.length;
            int[][] indicesForMvM = model.indicesForMxM;
            labelCodings = new DenseVector[nClass];
            for (int i = 0; i < nClass; ++i) {
                labelCodings[i] = new DenseVector(model_size);
            }
            for (int i = 0; i < model_size; ++i) {
                for (int j = 0; j < model.splitPoints[i]; ++j) {
                    labelCodings[indicesForMvM[i][j]].set(i, 1.0);
                }
                for (int j = model.splitPoints[i]; j < nClass; ++j) {
                    if (indicesForMvM[i][j] != -1)
                        labelCodings[indicesForMvM[i][j]].set(i, -1.0);
                }
            }
        }
    }


    public void loadModel(LinearModel linearModel) {
        this.model = linearModel;
        String[] featureNames = this.model.getFeatureNames();
        if (!model.isSparse) {
            this.featureN = featureNames.length;
            this.featureIdx = new int[this.featureN];
            String[] predictTableColNames = dataSchema.getColumnNames();
            for (int i = 0; i < this.featureN; i++) {
                this.featureIdx[i] = TableUtil.findIndexFromName(predictTableColNames, featureNames[i]);
            }
        }
        if (this.keepColNames != null) {
            this.keepN = this.keepColNames.length;
            this.keepIdx = new int[this.keepN];
            for (int i = 0; i < keepN; ++i) {
                keepIdx[i] = TableUtil.findIndexFromName(dataSchema.getColumnNames(), keepColNames[i]);
            }
        } else {
            this.keepN = dataSchema.getColumnNames().length;
            this.keepIdx = new int[this.keepN];
            for (int i = 0; i < keepN; ++i) {
                keepIdx[i] = i;
            }
        }

        out_size = this.keepN + 1;
        if (this.predScoreColName != null && this.predPositiveLabelValueString != null)
            out_size += 1;
        if (this.predDetailColName != null)
            out_size += 1;
    }


    @Override
    public Row predict(Row row) throws Exception {
        Row r = new Row(out_size);

        for (int i = 0; i < this.keepN; i++) {
            r.setField(i, row.getField(keepIdx[i]));
        }

        Vector aVector = LinearModel.getFeatureVector(row, model.isSparse, model.hasInterceptItem, this.featureN, this.featureIdx,
                this.tensorColIndex, model.sparseFeatureDim);

        int iter = keepN;
        if (model.linearModelType == LinearModelType.LR
                && (this.predDetailColName != null || this.predScoreColName != null)) {
            Tuple2<Object, Double[]> result = predictWithProb(aVector);
            r.setField(iter++, result.f0);
            if (this.predDetailColName != null) {
                Map<String, String> detail = new HashMap<>();
                int labelSize = model.labelValues.length;
                for (int i = 0; i < labelSize; ++i) {
                    detail.put(model.labelValues[i].toString(), result.f1[i].toString());
                }
                String json_detail = gson.toJson(detail);
                r.setField(iter++, json_detail);
            }

            if (this.predPositiveLabelValueString != null && this.predScoreColName != null) {
                if (predPositiveLabelValueString.equals(this.model.labelValues[0].toString()))
                    r.setField(iter, result.f1[0]);
                else
                    r.setField(iter, result.f1[1]);
            }

        } else { // only LR can give the probability. others still not support.
            r.setField(iter, predict(aVector));
        }

        return r;
    }

    public Object predict(Vector vector) {
        if (model.isMultiClass) {
            return predictMultiClass(vector);
        } else {
            double dotValue = VectorOp.dot(vector, model.coefVector);

            switch (model.linearModelType) {
                case LR:
                case SVM:
                case Perceptron:
                    return dotValue >= 0 ? model.labelValues[0] : model.labelValues[1];
                case LinearReg:
                case SVR:
                    return dotValue;
                default:
                    throw new RuntimeException("Not supported yet!");
            }
        }
    }

    private double sigmoid(double val) {
        return 1 - 1.0 / (1.0 + Math.exp(val));
    }

    public Tuple2<Object, Double[]> predictWithProb(Vector vector) {
        if (model.isMultiClass) {
            return predictMultiClassWithProb(vector);
        } else {
            double dotValue = VectorOp.dot(vector, model.coefVector);

            switch (model.linearModelType) {
                case LR:
                    double prob = sigmoid(dotValue);
                    return new Tuple2<>(dotValue >= 0 ? model.labelValues[0] : model.labelValues[1],
                            new Double[]{prob, 1 - prob});
                default:
                    throw new RuntimeException("not support score or detail yet!");
            }
        }
    }

    public Tuple2<Object, Double[]> predictMultiClassWithProb(Vector vector) {
        DenseVector[] coefVectors = model.getCoefVectors();
        int nClass = model.labelValues.length;
        Double[] probs = new Double[nClass];
        double[] dotValues = new double[nClass];
        double max_prob = -1.0;
        int max_idx = -1;

        if (model.multiClassType.equals("OneVsOne")) {

            for (int i = 0; i < coefVectors.length; ++i) {
                ImmutablePair<Integer, Integer> pair = this.map.get(i);
                double dotVal = VectorOp.dot(vector, coefVectors[i]);
                if (dotVal > 0) {
                    dotValues[pair.getKey()] += 1.0 / (nClass - 1);
                } else {
                    dotValues[pair.getValue()] += 1.0 / (nClass - 1);
                }
            }

            for (int i = 0; i < nClass; ++i) {
                probs[i] = dotValues[i];
                if (dotValues[i] > max_prob) {
                    max_prob = dotValues[i];
                    max_idx = i;
                }
            }
        } else {
            int labelSize = coefVectors.length;
            for (int i = 0; i < labelSize; ++i) {
                dotValues[i] = VectorOp.dot(vector, coefVectors[i]);

                probs[i] = sigmoid(dotValues[i]);
                if (probs[i] > max_prob) {
                    max_prob = probs[i];
                    max_idx = i;
                }
            }
        }
        switch (model.linearModelType) {
            case LR:
                return new Tuple2<>(model.labelValues[max_idx], probs);
            default:
                throw new RuntimeException("not support multi classification yet!");
        }
    }

    public Object predictMultiClass(Vector vector) {
        DenseVector[] coefVectors = model.getCoefVectors();

        double max_val = Double.NEGATIVE_INFINITY;
        int max_idx = -1;
        if (model.multiClassType.equals("OneVsOne")) {
            int nClass = model.labelValues.length;
            double[] dotValues = new double[nClass];

            for (int i = 0; i < coefVectors.length; ++i) {
                ImmutablePair<Integer, Integer> pair = this.map.get(i);
                double dotVal = VectorOp.dot(vector, coefVectors[i]);
                if (dotVal > 0) {
                    dotValues[pair.getKey()] += 1.0;
                } else {
                    dotValues[pair.getValue()] += 1.0;
                }
            }
            for (int i = 0; i < nClass; ++i) {
                if (dotValues[i] > max_val) {
                    max_val = dotValues[i];
                    max_idx = i;
                }
            }
        } else if (model.multiClassType.equals("OneVsRest")) {
            int nClass = coefVectors.length;
            double[] dotValues = new double[nClass];
            for (int i = 0; i < nClass; ++i) {
                dotValues[i] = VectorOp.dot(vector, coefVectors[i]);
                if (dotValues[i] > max_val) {
                    max_val = dotValues[i];
                    max_idx = i;
                }
            }
        } else if (model.multiClassType.equals("ManyVsMany")) {
            int nClass = model.labelValues.length;
            DenseVector dotValues = new DenseVector(model_size);
            for (int i = 0; i < model_size; ++i) {
                dotValues.set(i, (VectorOp.dot(vector, coefVectors[i]) > 0) ? 1.0 : -1.0);
            }
            // solve code
            // get every label's code vector with indicesForMvM
            // get the class which has the nearest distance
            double[] codeVals = new double[nClass];
            double min_val = Double.POSITIVE_INFINITY;
            for (int i = 0; i < nClass; ++i) {
                codeVals[i] = dotValues.minus(labelCodings[i]).l2normSquare();

                if (min_val > codeVals[i]) {
                    min_val = codeVals[i];
                    max_idx = i;
                }
            }
        }
        switch (model.linearModelType) {
            case LR:
            case SVM:
            case Perceptron:
                return model.labelValues[max_idx];
            default:
                throw new RuntimeException("not support multi classification yet!");
        }
    }


}
