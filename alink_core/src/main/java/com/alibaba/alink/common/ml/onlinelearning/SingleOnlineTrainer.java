package com.alibaba.alink.common.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.ml.LinearCoefInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;


public abstract class SingleOnlineTrainer implements Serializable {

    protected List <Row> modelRows;
    protected AlinkParameter params;
    protected TableSchema dataSchema;

    public SingleOnlineTrainer(List <Row> modelRows, TableSchema dataSchema, AlinkParameter params) {
        this.modelRows = modelRows;
        this.dataSchema = dataSchema;
        if (null == params) {
            this.params = new AlinkParameter();
        } else {
            this.params = params.clone();
        }
    }

    public void train(Iterable <Row> values) {
        train(values.iterator());
    }

    public abstract void train(Iterator <Row> iterator);

    public abstract LinearCoefInfo getCoefInfo();

    public abstract void setCoefInfo(LinearCoefInfo coefInfo);

//    public abstract DenseVector[] getWeights();
//
    public void setWeights(DenseVector weights) {
        LinearCoefInfo coefInfo = new LinearCoefInfo();
        coefInfo.coefVector = weights;
        setCoefInfo(coefInfo);
    }

//    public abstract void setWeights(DenseVector[] weights);

}
