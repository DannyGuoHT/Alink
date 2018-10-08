package com.alibaba.alink.common.utils;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.ml.LinearModelPredictor;
import com.alibaba.alink.common.ml.MLModelPredictor;
import com.alibaba.alink.common.utils.AlinkModel.ModelType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;

public class AlinkPredictorFactory {
    public static AlinkPredictor create(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        return create(modelScheme, dataSchema, params, null);
    }

    public static AlinkPredictor create(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params, List <Row> modelRows) {
        AlinkModel.assertModelSchema(modelScheme);
        if (params.contains(AlinkPredictor.CLASS_CANONICAL_NAME)) {

            String predictorClassName = params.getString(AlinkPredictor.CLASS_CANONICAL_NAME);
            if (predictorClassName.startsWith("com.alibaba.alink")) {
                try {
                    Class predictorClass = Class.forName(predictorClassName);
                    AlinkPredictor predictor = (AlinkPredictor) predictorClass
                            .getConstructor(TableSchema.class, TableSchema.class, AlinkParameter.class)
                            .newInstance(modelScheme, dataSchema, params);

                    if (null != modelRows) {
                        predictor.loadModel(modelRows);
                    }

                    return predictor;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new RuntimeException(ex.toString());
                }
            } else {
                throw new RuntimeException("Class must be in the package com.alibaba.alink");
            }
        } else {
            Tuple2 <ModelType, String> t2 = AlinkModel.getInfoWithModelSchema(modelScheme);
            switch (t2.f0) {
                case ML:
                    if (null != modelRows) {
                        return MLModelPredictor.of(modelScheme, dataSchema, params, modelRows);
                    } else {
                        return new LinearModelPredictor(modelScheme, dataSchema, params);
                    }
                default:
                    throw new RuntimeException("Not implemented yet!");
            }
        }
    }

}
