package com.alibaba.alink.common.outlier;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.utils.AlinkModel;

import org.apache.flink.types.Row;

public class SOSModel extends AlinkModel {

    private Double[] beta; // of each training samples
    private Double[] sumA; // of each training samples
    private DenseVector[] features; // of each training samples

    public SOSModel() {
    }

    public SOSModel(DenseVector[] features, Double[] beta, Double[] sumA) {
        this.features = features;
        this.beta = beta;
        this.sumA = sumA;
    }

    /**
     * Load model meta and data from a trained model table.
     * This interface is called by the predictor.
     *
     * @param rows
     */
    @Override
    public void load(List <Row> rows) {

        // must get the meta first
        for (Row row : rows) {
            if ((long)row.getField(0) == 0) {
                // the first row stores the meta of the model
                super.meta = AlinkParameter.fromJson((String)row.getField(1));
                break;
            }
        }

        if (super.meta == null) {
            throw new RuntimeException("fail to load SOS model meta");
        }

        int numTrainingSamples = super.meta.getInteger("numTrainingSamples");
        int numFeatures = super.meta.getInteger("numFeatures");

        this.beta = new Double[numTrainingSamples];
        this.features = new DenseVector[numTrainingSamples];
        for (int i = 0; i < numTrainingSamples; i++) {
            features[i] = new DenseVector(numFeatures);
        }
        this.sumA = new Double[numTrainingSamples];

        // get the model data
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                String data = (String)row.getField(1);
                int i = ((Long)row.getField(0)).intValue() - 1;
                String[] content = data.split(":", numFeatures + 2);
                for (int j = 0; j < numFeatures; j++) {
                    this.features[i].set(j, Double.valueOf(content[j]));
                }
                this.beta[i] = Double.valueOf(content[numFeatures]);
                this.sumA[i] = Double.valueOf(content[numFeatures + 1]);
            }
        }
    }

    /**
     * Save the model to a list of rows that conform to AlinkModel's standard format.
     * This is called by the train batch operator to save model to a table.
     *
     * @return
     */
    @Override
    public List <Row> save() { // save meta and data to a list of rows
        ArrayList <Row> list = new ArrayList <>();
        long id = 0L;

        // the first row: model meta
        list.add(Row.of(new Object[] {id++, super.meta.toJson(), "null", "null"}));

        // save the user names and their factors
        int n = features.length;
        for (int i = 0; i < n; i++) {
            int numFeatures = features[i].size();
            StringBuilder sbd = new StringBuilder();
            for (int j = 0; j < numFeatures; j++) {
                sbd.append(Double.toString(features[i].get(j)));
                sbd.append(":");
            }
            sbd.append(Double.toString(beta[i]));
            sbd.append(":");
            sbd.append(Double.toString(sumA[i]));
            list.add(Row.of(new Object[] {id++, sbd.toString(), "null", "null"}));
        }

        return list;
    }

    public Row predict(Row row, int[] featureColIndices, final int[] keepColIdx) {
        int numFeatures = featureColIndices.length;
        int numSamples = this.features.length;

        if (numFeatures != this.features[0].size()) { throw new RuntimeException("mismatched number of features"); }

        int keepSize = keepColIdx.length;
        Row r = new Row(keepSize + 1);
        for (int i = 0; i < keepSize; i++) {
            r.setField(i, row.getField(keepColIdx[i]));
        }

        double[] x = new double[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            x[i] = ((Number)row.getField(featureColIndices[i])).doubleValue();
        }

        double[] affinity = new double[numSamples];
        double prob = 1.0;

        for (int i = 0; i < numSamples; i++) {
            double dsquare = 0.;
            for (int j = 0; j < numFeatures; j++) {
                dsquare += (this.features[i].get(j) - x[j]) * (this.features[i].get(j) - x[j]);
            }
            affinity[i] = Math.exp(-dsquare * this.beta[i]);
            double probI = affinity[i] / (affinity[i] + this.sumA[i]);
            prob *= 1.0 - probI;
        }

        r.setField(keepSize, prob);
        return r;
    }
}
