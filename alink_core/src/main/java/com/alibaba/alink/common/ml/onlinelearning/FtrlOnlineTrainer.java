package com.alibaba.alink.common.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.common.matrix.VectorIterator;
import com.alibaba.alink.common.ml.LinearCoefInfo;
import com.alibaba.alink.common.ml.LinearModel;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.List;

public class FtrlOnlineTrainer extends SingleOnlineTrainer {

    private LinearModel model = null;

    private int[] featureColIndices;
    private String positiveLabelValueString;
    private int labelColIndex;

    private String[] labelValueStrings = null;

    private double alpha = 0.1;
    private double beta = 1;
    private double L1 = 1;
    private double L2 = 1;

    private int nFeature;
    private boolean isSparse;
    private boolean hasInterceptItem;
    private int tensorColIndex = -1;
    private int sparseFeatureDim;
    private boolean isRegProc;

    private double[][] N; // 存放累加的w
    private double[][] Z; // sum of the gradient^2
//    private double[] weights; // 模型最后的参数

    public FtrlOnlineTrainer(List <Row> modelRows, TableSchema dataSchema, AlinkParameter params) {
        super(modelRows, dataSchema, params);

        alpha = this.params.getDoubleOrDefault("alpha", 0.1);
        beta = this.params.getDoubleOrDefault("beta", 1.0);
        L1 = this.params.getDoubleOrDefault("l1", this.params.getDoubleOrDefault("L1", 1.0));
        L2 = this.params.getDoubleOrDefault("l2", this.params.getDoubleOrDefault("L2", 1.0));

        String labelColName = this.params.getString(ParamName.labelColName);

        this.model = new LinearModel();
        this.model.load(this.modelRows);

        Object[] labelValues = this.model.getlabelValues();
        positiveLabelValueString = null;
        if (null != labelValues && labelValues.length > 0) {
            positiveLabelValueString = labelValues[0].toString();
        }
        if (model.isMultiClass) {
            labelValueStrings = new String[labelValues.length];
            for (int i = 0; i < labelValueStrings.length; i++) {
                labelValueStrings[i] = labelValues[i].toString();
            }
        }

        int nClass = 1;
        int nCoef = 0;
        if (model.isMultiClass) {
            nClass = model.coefVectors.length;
            nCoef = model.coefVectors[0].size();
        } else {
            nCoef = model.coefVector.size();
        }
        N = new double[nClass][nCoef];
        Z = new double[nClass][nCoef];

        String[] dataColNames = dataSchema.getColumnNames();
        labelColIndex = TableUtil.findIndexFromName(dataColNames, labelColName);
        String[] featureColNames = this.model.getFeatureNames();
        if (null != featureColNames) {
            this.nFeature = featureColNames.length;
            featureColIndices = new int[nFeature];
            for (int i = 0; i < nFeature; i++) {
                featureColIndices[i] = TableUtil.findIndexFromName(dataColNames, featureColNames[i]);
            }
        }

        this.isSparse = model.isSparse();
        this.hasInterceptItem = model.hasInterceptItem();
        if (null != params) {
            String tensorColName = params.getStringOrDefault(ParamName.tensorColName, null);
            if (null != tensorColName) {
                this.tensorColIndex = TableUtil.findIndexFromName(dataSchema.getColumnNames(), tensorColName);
            }
        }
        this.sparseFeatureDim = model.sparseFeatureDim;

        this.isRegProc = false;
        switch (model.getLinearModelType()) {
            case LinearReg:
            case SVR:
                this.isRegProc = true;
        }
    }

    @Override
    public void train(Iterator <Row> iterator) {

        while (iterator.hasNext()) {
            Row row = iterator.next();

            Vector featureVector = LinearModel.getFeatureVector(row, this.isSparse, this.hasInterceptItem, this.nFeature, this.featureColIndices,
                    this.tensorColIndex, this.sparseFeatureDim);

            if (model.isMultiClass) {
                String targetValueString = row.getField(labelColIndex).toString();
                for (int i = 0; i < this.labelValueStrings.length; i++) {
                    double target = LinearModel.getLabelValue(targetValueString, labelValueStrings[i]);
                    target = target > 0.5 ? 1 : 0;
                    updateProc(model.coefVectors[i], featureVector, target, i);
                }
            } else {
                double target = LinearModel.getLabelValue(row, this.isRegProc, this.labelColIndex, this.positiveLabelValueString);
                target = target > 0.5 ? 1 : 0;
                updateProc(model.coefVector, featureVector, target, 0);
            }

        }

    }

    @Override
    public LinearCoefInfo getCoefInfo() {
        return model.getCoefInfo();
    }

    @Override
    public void setCoefInfo(LinearCoefInfo coefInfo) {
        model.setCoefInfo(coefInfo);
    }

    private void updateProc(DenseVector weights, Vector featureVector, double label, int index) {
        double p = 0.0;
        VectorIterator vi = featureVector.iterator();
        while (vi.hasNext()) {
            int i = vi.getIndex();
            if (Math.abs(Z[index][i]) <= L1) {
                weights.set(i, 0.0);
            } else {
                weights.set(i, ((Z[index][i] < 0 ? -1 : 1) * L1 - Z[index][i]) / ((beta + Math.sqrt(N[index][i])) / alpha + L2));
            }
            p += weights.get(i) * vi.getValue();
            vi.next();
        }
        // eta
        p = 1 / (1 + Math.exp(-p));

        // update
        vi = featureVector.iterator();
        while (vi.hasNext()) {
            int i = vi.getIndex();
            double g = (p - label) * vi.getValue();
            double sigma = (Math.sqrt(N[index][i] + g * g) - Math.sqrt(N[index][i])) / alpha;
            Z[index][i] += g - sigma * weights.get(i);
            N[index][i] += g * g;
            vi.next();
        }

//
//        double p = 0.0;
//        for (int k = 0; k < indexes.length; k++) {
//            int i = indexes[k];
//            if (Math.abs(Z[i]) <= L1) {
//                weights[i] = 0.0;
//            } else {
//                weights[i] = ((Z[i] < 0 ? -1 : 1) * L1 - Z[i])
//                        / ((beta + Math.sqrt(N[i])) / alpha + L2);
//            }
//            p += weights[i] * values[k];
//        }
//        // eta
//        p = 1 / (1 + Math.exp(-p));
//
//        // update
//        for (int k = 0; k < indexes.length; k++) {
//            int i = indexes[k];
//            double g = (p - labels) * values[k];
//            double sigma = (Math.sqrt(N[i] + g * g) - Math.sqrt(N[i])) / alpha;
//            Z[i] += g - sigma * weights[i];
//            N[i] += g * g;
//        }
    }

//    private void updateProc(double[] features, double target) {
//        double p = 0.0;
//        for (int i = 0; i < nFeature; i++) {
//            if (Math.abs(Z[i]) <= L1) {
//                weights[i] = 0.0;
//            } else {
//                weights[i] = ((Z[i] < 0 ? -1 : 1) * L1 - Z[i])
//                        / ((beta + Math.sqrt(N[i])) / alpha + L2);
//            }
//            p += weights[i] * features[i];
//        }
//        // eta
//        p = 1 / (1 + Math.exp(-p));
//
//        // update
//        for (int i = 0; i < nFeature; i++) {
//            double g = (p - target) * features[i];
//            double sigma = (Math.sqrt(N[i] + g * g) - Math.sqrt(N[i])) / alpha;
//            Z[i] += g - sigma * weights[i];
//            N[i] += g * g;
//        }
//    }

}
