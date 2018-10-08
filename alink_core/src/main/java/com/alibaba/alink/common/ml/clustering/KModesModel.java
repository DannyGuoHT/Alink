package com.alibaba.alink.common.ml.clustering;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.string.DenseStringVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.AlinkSession.gson;


public class KModesModel extends AlinkModel {

    /**
     * predict scope (after loading, before predicting)
     */
    private int k;

    /**
     * Tuple3: clusterId, clusterWeight, clusterCentroid
     */
    private List<Tuple3<Long, Double, DenseStringVector>> centroids;

    /**
     * train scope (after train, before saving model)
     */
    private Iterable<Tuple3<Long, Double, DenseStringVector>> data = null;

    public KModesModel() {
    }

    public KModesModel(Iterable<Tuple3<Long, Double, DenseStringVector>> data) {
        this.data = data;
    }

    public List<Tuple3<Long, Double, DenseStringVector>> getCentroids() {
        return centroids;
    }

    public static long findCluster(Iterable<Tuple3<Long, Double, DenseStringVector>> centroids, DenseStringVector sample) {
        long clusterId = -1;
        double d = Double.POSITIVE_INFINITY;

        for (Tuple3<Long, Double, DenseStringVector> c : centroids) {
            double distance = Distance.calcCategoricalDistance(CategoricalDistanceType.ONEZERO,sample,c.f2);
            if (distance < d) {
                clusterId = c.f0;
                d = distance;
            }
        }
        return clusterId;
    }

    public static Tuple2<Long,Double> getCluster(Iterable<Tuple3<Long, Double, DenseStringVector>> centroids, DenseStringVector sample) {
        long clusterId = -1;
        double d = Double.POSITIVE_INFINITY;

        for (Tuple3<Long, Double, DenseStringVector> c : centroids) {
            double distance = Distance.calcCategoricalDistance(CategoricalDistanceType.ONEZERO,sample,c.f2);
            if (distance < d) {
                clusterId = c.f0;
                d = distance;
            }
        }
        return new Tuple2<>(clusterId,d);
    }


    /**
     * Load model meta and data from a trained model table.
     * This interface is called by the predictor.
     *
     * @param rows
     */
    @Override
    public void load(List<Row> rows) {
        // must get the meta first
        for (Row row : rows) {
            if ((long) row.getField(0) == 0) {
                // the first row stores the meta of the model
                this.meta = AlinkParameter.fromJson((String) row.getField(1));
                break;
            }
        }

        if (this.meta == null) {
            throw new RuntimeException("fail to load KMeans model meta");
        }

        this.k = this.meta.getInteger(ParamName.k);
        this.centroids = new ArrayList<>(this.k);

        // get the model data
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                // the first row stores the meta of the model
                continue;
            } else {
                Tuple3<Long, Double, DenseStringVector> centroid =
                        new Tuple3<>(Long.valueOf((String) row.getField(1)),
                                Double.valueOf((String) row.getField(2)),
                                gson.fromJson((String) row.getField(3), DenseStringVector.class));
                this.centroids.add(centroid);
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
    public List<Row> save() { // save meta and data to a list of rows
        ArrayList<Row> list = new ArrayList<>();

        // the first row: model meta
        list.add(Row.of(0L, super.meta.toJson(), null, null));

        long id = 1L;
        for (Tuple3<Long, Double, DenseStringVector> centroid : data) {
            String str = gson.toJson(centroid.f2);
            list.add(Row.of(id++, String.valueOf(centroid.f0), String.valueOf(centroid.f1), str));
        }

        return list;
    }

    /**
     * Predict
     */
    public Row predict(Row row, final int[] colIdx, final int[] keepColIdx) throws Exception {
        int keepSize = keepColIdx.length;
        Row r = new Row(keepSize + 1);
        for (int i = 0; i < keepSize; i++) {
            r.setField(i, row.getField(keepColIdx[i]));
        }

        String[] record = new String[colIdx.length];
        for (int i = 0; i < record.length; i++) {
            record[i] = (String.valueOf(row.getField(colIdx[i])));
        }

        long clusterId = findCluster(this.centroids, new DenseStringVector(record));
        r.setField(keepSize, clusterId);

        return r;
    }
}
