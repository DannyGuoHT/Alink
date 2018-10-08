package com.alibaba.alink.batchoperator.utils;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.matrix.SparseVector;
import com.alibaba.alink.common.matrix.Tensor;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.common.utils.Util;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

public class DataConvert {

    /**
     * convert table, dense tensor or sparse tensor to dense vector.
     *
     * @param in               input batch op
     * @param selectedColNames required when data is table, else set null
     * @param tensorColName    required when data is tensor, else set null
     * @param isSparse         optional. if isSparse is true, then it is sparse tensor,else it is dense tensor.
     * @return DataSet
     */
    public static DataSet<Row> toDenseVector(BatchOperator in, String[] selectedColNames,
                                             String tensorColName, boolean isSparse) {
        /*
         * if featureColNames is not null, then is table;
         * if featureColNames is null and tensorColName not null, it is tensor.
         */
        boolean isTensor = Util.isTensor(selectedColNames, tensorColName);

        /*
         *  if table, featureIdxs is indexs of selectedColNames in input data;
         *  if tensor, featureIdxs is indexs of tensorColName in input data;
         */
        int[] featureIdxs = Util.getTensorColIdxs(selectedColNames, tensorColName,
                in.getColNames(), in.getColTypes());

        DataSet<Row> data = null;

        //parse tensor or table to dense vector
        if (isTensor && isSparse) {
            //parse to sparse vector
            SparseVecMap sparseVecMap = new SparseVecMap(featureIdxs[0]);
            DataSet<SparseVector> sparseVec = in.getDataSet().map(sparseVecMap);

            //get sparse vector feature dim
            DataSet<Integer> sparseFeatureDim = DataConvert.getSparseTensorDim(sparseVec);

            //to dense vector
            Sparse2DenseMap sparse2DenseMap = new Sparse2DenseMap();
            data = sparseVec.map(sparse2DenseMap).withBroadcastSet(sparseFeatureDim, "featureDim");
        } else {
            //convert table or dense tensor to dense vector
            TransferDenseMap transferDenseMap = new TransferDenseMap(isTensor, featureIdxs);
            data = in.getDataSet().rebalance()
                    .map(transferDenseMap);
        }

        return data;
    }

    /**
     * get feature dim from sparse vector
     * @param sparseVec
     * @return
     */

    public static DataSet<Integer> getSparseTensorDim(DataSet<SparseVector> sparseVec) {
        return sparseVec.map(new MapFunction<SparseVector, Integer>() {
            @Override
            public Integer map(SparseVector value) throws Exception {
                return value.getMaxIndex() + 1;
            }
        }).reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1,
                                  Integer value2) throws Exception {
                return Math.max(value1, value2);
            }
        });
    }
}

/**
 * parse tensor string to sparse vector
 */
class SparseVecMap implements MapFunction<Row, SparseVector> {
    private int colIdx;

    public SparseVecMap(int colIdx) {
        this.colIdx = colIdx;
    }

    @Override
    public SparseVector map(Row in) throws Exception {
        return Tensor.parse(in.getField(colIdx).toString()).toSparseVector();
    }
}

/**
 * convert sparse tensor to dense vector, fill zero in missing value, given featureDim
 */
class Sparse2DenseMap extends RichMapFunction<SparseVector, Row> {
    private int sparseFeatureDim;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.sparseFeatureDim = (Integer) getRuntimeContext()
                .getBroadcastVariable("featureDim").get(0);
    }

    @Override
    public Row map(SparseVector in) throws Exception {
        Row out = new Row(sparseFeatureDim);
        for (int i = 0; i < sparseFeatureDim; i++) {
            out.setField(i, 0);
        }
        for (int i = 0; i < in.indices.length; i++) {
            out.setField(in.indices[i], in.values[i]);
        }
        return out;
    }
}

/**
 * convert table or dense tensor to double dense vector
 */
class TransferDenseMap implements MapFunction<Row, Row> {

    private boolean isTensor;
    private int[] featureIdxs;

    public TransferDenseMap(boolean isTensor, int[] featureIdxs) {
        this.isTensor = isTensor;
        this.featureIdxs = featureIdxs;
    }

    @Override
    public Row map(Row in) throws Exception {
        Row out = null;
        if (isTensor) {
            //dense tensor
            String str = in.getField(featureIdxs[0]).toString();
            Vector feature = Tensor.parse(str).toDenseVector();
            out = new Row(feature.size());
            for (int i = 0; i < feature.size(); i++) {
                out.setField(i, feature.get(i));
            }
        } else {
            //table
            out = new Row(this.featureIdxs.length);
            for (int i = 0; i < this.featureIdxs.length; ++i) {
                out.setField(i, ((Number) in.getField(this.featureIdxs[i])).doubleValue());
            }
        }
        return out;
    }
}
