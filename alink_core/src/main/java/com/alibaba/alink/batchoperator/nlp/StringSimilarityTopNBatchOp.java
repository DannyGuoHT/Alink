package com.alibaba.alink.batchoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.nlp.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;



public class StringSimilarityTopNBatchOp extends BatchOperator implements Serializable {
    public StringSimilarityTopNBatchOp(String inputSelectedColName, String mapSelectedColName, String method,
                                       Integer k, Double lambda, Integer seed, Integer minHashK, Integer bucket,
                                       Integer topN, String[] leftKeepColNames, String[] rightKeepColNames) {
        this(new AlinkParameter()
                .put("inputSelectedColName", inputSelectedColName)
                .put("mapSelectedColName", mapSelectedColName)
                .put("method", method)
                .put("k", k)
                .put("lambda", lambda)
                .put("seed", seed)
                .put("minHashK", minHashK)
                .put("bucket", bucket)
                .put("topN", topN)
                .put("leftKeepColNames", leftKeepColNames)
                .put("rightKeepColNames", rightKeepColNames));
    }

    public StringSimilarityTopNBatchOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in){
        throw new RuntimeException("Need 2 inputs!");
    }

    @Override
    public BatchOperator linkFrom(List<BatchOperator> ins)  {
        if (null == ins ) {
            throw new RuntimeException("No inputs");
        }
        if(ins.size() > 2) {
            throw new RuntimeException("Support 1 or 2 inputs");
        }

        String inputSelectedColName = this.params.getStringOrDefault("inputSelectedColName", null);
        String mapSelectedColName = this.params.getStringOrDefault("mapSelectedColName", null);
        String outputColName = this.params.getStringOrDefault(ParamName.outputColName, StringSimilarityConst.OUTPUT);
        String[] leftKeepColNames = this.params.getStringArrayOrDefault("leftKeepColNames", StringSimilarityConst.KEEPCOLNAMES);
        String[] rightKeepColNames = this.params.getStringArrayOrDefault("rightKeepColNames", StringSimilarityConst.KEEPCOLNAMES);

        OutputTableInfo outTable = new OutputTableInfo();
        outTable.getOutputTableInfo(
                ins.get(0).getColNames(),
                ins.get(0).getColTypes(),
                ins.get(1).getColNames(),
                ins.get(1).getColTypes(),
                inputSelectedColName,
                mapSelectedColName,
                outputColName,
                leftKeepColNames,
                rightKeepColNames);

        DataSet<Row> left = ins.get(0).select(outTable.getLeftSelectedColNames()).getDataSet();

        DataSet<Row> right = ins.get(1).select(outTable.getRightSelectedColNames()).getDataSet();

        StringSimilarityTopNBatchBase base = new StringSimilarityTopNBatchBase(this.params, false);

        DataSet<Row> out = base.stringSimilarityRes(left, right);

        this.table = RowTypeDataSet.toTable(out, outTable.getOutColNames(), outTable.getOutTypes());
        return this;
    }

}
