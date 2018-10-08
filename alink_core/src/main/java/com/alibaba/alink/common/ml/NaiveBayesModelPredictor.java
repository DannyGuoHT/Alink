package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseMatrix;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.SparseVector;
import com.alibaba.alink.common.matrix.Tensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.AlinkSession.gson;

/**
 * *
 * this class receive a sample stream and predict the samples.
 *
 */
public class NaiveBayesModelPredictor extends MLModelPredictor {

    private String modelType;
    private int[] feat_idx;

    private DenseMatrix theta;
    private double[] pi;
    private Object[] label;
    private int feat_len;
    private String labelType;

    private String[] colNames;
    private String tensorColName;
    private boolean isSparse;
    private int tensorIndex;
    private int out_size;

    /**
     * construct function
     * @param modelScheme model schema
     * @param dataSchema  data schema
     * @param params      parameters for predict
     */
    public NaiveBayesModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        this.colNames = dataSchema.getColumnNames();
        this.tensorColName = this.params.getStringOrDefault(ParamName.tensorColName, null);

        if (this.tensorColName != null && this.tensorColName.length() != 0)
            this.tensorIndex = TableUtil.findIndexFromName(this.colNames, this.tensorColName);
        if (this.keepColNames != null) {
            this.keepN = keepColNames.length;
            this.keepIdx = new int[this.keepColNames.length];
            for (int i = 0; i < keepIdx.length; ++i) {
                keepIdx[i] = TableUtil.findIndexFromName(this.colNames, keepColNames[i]);
            }
        } else {
            this.keepN = dataSchema.getColumnNames().length;
        }

        out_size = this.keepN + 1;
        if (this.predDetailColName != null) {
            out_size += 1;
        }
    }

    private double[] multinomialCalculation(double[] vec) {
        int row_size = this.theta.getRowDimension();
        int col_size = this.theta.getColumnDimension();
        double[] prob = new double[row_size];
        for (int i = 0; i < row_size; ++i) {
            for (int j = 0; j < col_size; ++j) {
                prob[i] += this.theta.get(i, j) * vec[j];
            }
        }
        for (int j = 0; j < row_size; ++j) {
            prob[j] += this.pi[j];
        }
        return prob;
    }

    private double[] bernoulliCalculation(double[] vec) {
        int row_size = this.theta.getRowDimension();
        int col_size = this.theta.getColumnDimension();
        double[] prob = new double[row_size];
        for (int j = 0; j < col_size; ++j) {
            if ((vec[j] == 1.0 || vec[j] == 0.0)) {
                continue;
            } else {
                throw new RuntimeException("Bernoulli naive Bayes requires 0 or 1 feature values.");
            }
        }

        double[] tmp_val = new double[row_size];
        DenseMatrix minMat = new DenseMatrix(row_size, col_size);
        for (int i = 0; i < row_size; ++i) {
            for (int j = 0; j < col_size; ++j) {
                tmp_val[i] += Math.log(1 - Math.exp(this.theta.get(i, j)));
                minMat.set(i, j, theta.get(i, j) - Math.log(1 - Math.exp(this.theta.get(i, j))));
            }
        }

        for (int i = 0; i < row_size; ++i) {
            for (int j = 0; j < col_size; ++j) {
                prob[i] += minMat.get(i, j) * vec[j];
            }
            prob[i] += tmp_val[i];
            prob[i] += this.pi[i];
        }
        return prob;
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        NaiveBayesModel model = new NaiveBayesModel();
        model.load(modelRows);
        String[] featureNames = model.getMetaInfo()
                .getStringArrayOrDefault(ParamName.featureColNames, null);

        this.modelType = model.getMetaInfo().getString("bayesType");
        this.labelType = (model.getMetaInfo()).getString(ParamName.labelType);
        this.isSparse = model.getMetaInfo().getBoolOrDefault(ParamName.isSparse, false);
        if (this.isSparse) {
            this.feat_len = model.getMetaInfo().getIntegerOrDefault("featureDim", -1);
        }

        int size = 1;
        if (featureNames != null) {
            size = featureNames.length;
            feat_idx = new int[size];
            for (int i = 0; i < size; ++i) {
                feat_idx[i] = TableUtil.findIndexFromName(colNames, featureNames[i]);
            }
        } else {
            feat_idx = new int[1];
            feat_idx[0] = TableUtil.findIndexFromName(colNames, tensorColName);
        }
        this.modelType = modelType;
        NaiveBayesProbInfo data = model.getData();

        Object[] lab = data.labels;
        this.theta = data.theta;

        int data_size = lab.length;
        this.pi = data.piArray;
        this.feat_len = theta.getColumnDimension();

        // transfrom the label type
        AlinkParameter labelParam = new AlinkParameter()
                .put(ParamName.labelValues, lab)
                .put(ParamName.labelType, this.labelType, String.class);

        Tuple2<TypeInformation<?>, Object[]> labelValues = model.getLableTypeValues(labelParam);
        this.label = labelValues.f1;

    }

    @Override
    public Row predict(Row src) throws Exception {
        int iter = 0;
        Row new_row = new Row(out_size);
        if (this.keepColNames != null) {
            for (int i = 0; i < this.keepN; ++i) {
                new_row.setField(i, src.getField(this.keepIdx[i]));
            }
            iter += this.keepN;
        } else {
            int size = src.getArity();
            for (int i = 0; i < size; ++i) {
                new_row.setField(i, src.getField(i));
            }
            iter += size;
        }
        double[] feat_vec = new double[feat_len];

        if (isSparse) {
            String str = src.getField(this.tensorIndex).toString();

            SparseVector feat = Tensor.parse(str).toSparseVector();
            double[] val = feat.values;
            int[] idx = feat.indices;
            for (int i = 0; i < val.length; ++i) {
                feat_vec[idx[i]] = val[i];
            }
        } else if (tensorColName != null){
            String str = src.getField(this.tensorIndex).toString();

            DenseVector feat = Tensor.parse(str).toDenseVector();
            feat_vec = feat.toDoubleArray();
        }else {
            feat_vec = new double[this.feat_len];
            for (int i = 0; i < this.feat_len; ++i) {
                feat_vec[i] = Double.parseDouble(src.getField(feat_idx[i]).toString());
            }
        }
        double[] prob;
        if (this.modelType.equals("Multinomial"))
            prob = multinomialCalculation(feat_vec);
        else
            prob = bernoulliCalculation(feat_vec);

        if (this.predDetailColName != null) {
            double max_prob = prob[0];
            for (int i = 1; i < prob.length; ++i) {
                if (max_prob < prob[i]) {
                    max_prob = prob[i];
                }
            }
            double sumProb = 0.0;
            for (int i = 0; i < prob.length; ++i) {
                sumProb += Math.exp(prob[i] - max_prob);
            }
            sumProb = max_prob + Math.log(sumProb);
            for (int i = 0; i < prob.length; ++i) {
                prob[i] = Math.exp(prob[i] - sumProb);
            }
        }

        int prob_size = prob.length;
        double max_val = Double.NEGATIVE_INFINITY;
        Object result = null;
        for (int i = 0; i < prob_size; ++i) {
            if (max_val < prob[i]) {
                max_val = prob[i];
                result = this.label[i];
            }
        }

        new_row.setField(iter++, result);

        if (this.predDetailColName != null) {
            Map<String, String> detail = new HashMap<>();
            int labelSize = pi.length;
            for (int i = 0; i < labelSize; ++i) {
                detail.put(label[i].toString(), ((Double) prob[i]).toString());
            }
            String json_detail = gson.toJson(detail);
            new_row.setField(iter++, json_detail);
        }
        return new_row;
    }
}