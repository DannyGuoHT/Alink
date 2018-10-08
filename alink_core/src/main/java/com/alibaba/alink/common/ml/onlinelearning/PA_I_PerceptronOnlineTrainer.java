package com.alibaba.alink.common.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.matrix.Vector;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;

public class PA_I_PerceptronOnlineTrainer extends PerceptronBaseTrainer {

    public PA_I_PerceptronOnlineTrainer(List <Row> modelRows, TableSchema dataSchema, AlinkParameter params) {
        super(modelRows, dataSchema, params);
    }

    @Override
    public double calTau(Vector features, double target, double pred, double C) {
        double sufferLoss = Math.max(0.0, 1 - target * pred);
        return Math.min(C, sufferLoss / features.l2normSquare());
    }

}
