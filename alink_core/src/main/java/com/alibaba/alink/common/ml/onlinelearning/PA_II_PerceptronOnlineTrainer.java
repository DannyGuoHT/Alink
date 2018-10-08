package com.alibaba.alink.common.ml.onlinelearning;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.matrix.Vector;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;

public class PA_II_PerceptronOnlineTrainer extends PerceptronBaseTrainer {

    public PA_II_PerceptronOnlineTrainer(List <Row> modelRows, TableSchema dataSchema, AlinkParameter params) {
        super(modelRows, dataSchema, params);
    }

    @Override
    public double calTau(Vector features, double target, double pred, double C) {
        double sufferLoss = Math.max(0.0, 1 - target * pred);
        return sufferLoss / (features.l2normSquare() + 0.5 / C);
    }
}
