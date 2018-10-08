package com.alibaba.alink.common.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.common.matrix.VectorOp;
import com.alibaba.alink.common.ml.LinearCoefInfo;
import com.alibaba.alink.common.ml.LinearModel;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.List;


public abstract class PerceptronBaseTrainer extends SingleOnlineTrainer {

    private LinearModel model = null;

    private int[] featureColIndices;

    private int labelColIndex;
    private String positiveLabelValueString;

    private String[] labelValueStrings = null;

    protected Double C;

    private int nFeature;
    private boolean isSparse;
    private boolean hasInterceptItem;
    private int tensorColIndex;
    private int sparseFeatureDim;
    private boolean isRegProc;

    protected PerceptronBaseTrainer(List <Row> modelRows, TableSchema dataSchema, AlinkParameter params) {
        super(modelRows, dataSchema, params);

        // the constructor of SingleOnlineTrainer is called in the 'open()' of a window function,
        // so the coefVector should be loaded here

        AlinkParameter meta = AlinkModel.getMetaParams(modelRows);

        this.C = params.getDoubleOrDefault("C", null);
        this.model = new LinearModel();
        this.model.load(this.modelRows);

        String labelColName = this.params.getString(ParamName.labelColName);

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
                    updateProc(model.coefVectors[i], featureVector, target, this.C);
                }
            } else {
                double target = LinearModel.getLabelValue(row, this.isRegProc, this.labelColIndex, this.positiveLabelValueString);
                updateProc(model.coefVector, featureVector, target, this.C);
            }
        }

    }

    private void updateProc(DenseVector coefVector, Vector features, double target, double C) {
        double pred = VectorOp.dot(coefVector, features);
        double tau = calTau(features, target, pred, C);
        if (0.0 != tau) {
            coefVector.plusScaleEqual(features, tau * target);
        }
    }

    public abstract double calTau(Vector features, double target, double pred, double C);
//    double sufferLoss = Math.max(0.0, 1 - target * pred);
//    {
//        switch (this.type) {
//            case Perceptron:
//                tau = (Math.signum(pred) * target > 0) ? 0.0 : 1.0;
//                break;
//            case PA:
//                tau = sufferLoss / features.l2normSquare();
//                break;
//            case PA_I:
//                tau = Math.min(C, sufferLoss / features.l2normSquare());
//                break;
//            case PA_II:
//                tau = sufferLoss / (features.l2normSquare() + 0.5 / C);
//                break;
//            default:
//                throw new RuntimeException("Not supported yet!");
//        }
//    }


    @Override
    public LinearCoefInfo getCoefInfo() {
        return model.getCoefInfo();
    }

    @Override
    public void setCoefInfo(LinearCoefInfo coefInfo) {
        model.setCoefInfo(coefInfo);
    }

}
