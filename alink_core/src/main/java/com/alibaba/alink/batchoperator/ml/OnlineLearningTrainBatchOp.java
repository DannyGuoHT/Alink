package com.alibaba.alink.batchoperator.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.ml.LinearModel;
import com.alibaba.alink.common.ml.LinearModelType;
import com.alibaba.alink.common.ml.MLModel;
import com.alibaba.alink.common.ml.MLModelType;
import com.alibaba.alink.common.ml.onlinelearning.SingleOnlineTrainer;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class OnlineLearningTrainBatchOp extends BatchOperator {

    private String modelName;
    private String[] featureColNames;
    private String labelName;
    private String weightColName = null;

    private DataSet <Row> labelValues = null;
    private String positiveLabelValueString = null;

    private TypeInformation <?> labelType = null;
    private LinearModelType linearModelType;

    private boolean hasInterceptItem;

    private Integer sparseFeatureDim = -1;
    private boolean isSparse = false;


    public OnlineLearningTrainBatchOp(Class trainerClass, AlinkParameter params) {
        super(params);
        this.params.put(AlinkPredictor.CLASS_CANONICAL_NAME, trainerClass.getCanonicalName())
                .put(ParamName.modelName, "OnlineLearning")
                .put(ParamName.linearModelType, LinearModelType.LR);

        String[] requiredParams = new String[]{ParamName.modelName, ParamName.featureColNames,
                ParamName.labelColName, ParamName.linearModelType};
        if (!this.params.contains(requiredParams)) {
            throw new RuntimeException("Missed one or more required parameters!");
        }
    }


    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        this.modelName = this.params.getString(ParamName.modelName);
        this.featureColNames = params.getStringArray(ParamName.featureColNames);
        this.labelName = params.getString(ParamName.labelColName);

        // add weight for samples
        this.weightColName = params.getStringOrDefault(ParamName.weightColName, null);
        this.linearModelType = params.get(ParamName.linearModelType, LinearModelType.class);
        this.hasInterceptItem = params.getBoolOrDefault(ParamName.hasInterceptItem, Boolean.TRUE);

        this.sparseFeatureDim = params.getIntegerOrDefault(ParamName.sparseFeatureDim, null);
        this.isSparse = params.getBoolOrDefault(ParamName.isSparse, false);

        boolean isRegProc = false;
        switch (this.linearModelType) {
            case LinearReg:
            case SVR:
                isRegProc = true;
        }

        if (null == this.featureColNames) {
            this.featureColNames = TableUtil.getNumericColNames(in.getSchema(), new String[]{this.labelName});
        }

        if (isRegProc) {
            this.labelType = Types.DOUBLE;
            this.labelValues = null;
            this.positiveLabelValueString = null;

            List <Row> rows = new ArrayList <>();
            rows.add(Row.of(1));
            this.labelValues = AlinkSession.getExecutionEnvironment().fromCollection(rows);
        } else {
            if (null == this.labelType) {
                this.labelType = in.getColTypes()[TableUtil.findIndexFromName(in.getColNames(), this.labelName)];
            }
            this.labelValues = in
                    .select(this.labelName)
                    .distinct()
                    .getDataSet();
        }


        DataSet <Row> initModelRows = labelValues
                .mapPartition(new CreateInitModel(this.modelName,
                        this.linearModelType, this.labelType, this.hasInterceptItem,
                        this.isSparse, isRegProc,
                        this.positiveLabelValueString, this.featureColNames))
                .setParallelism(1);


        DataSet <Row> modelRows = in.getDataSet()
                .mapPartition(new OnlineTrainMapPartFunc(TableUtil.toSchemaJson(in.getSchema()), this.params))
                .withBroadcastSet(initModelRows, "InitModelRows")
                .setParallelism(1);

        this.table = RowTypeDataSet.toTable(modelRows, MLModel.getModelSchemaWithType(this.labelType));
        return this;
    }

    public static class OnlineTrainMapPartFunc extends AbstractRichFunction
            implements MapPartitionFunction <Row, Row> {
        private SingleOnlineTrainer trainer = null;
        private String dataSchemaJson;
        private AlinkParameter params;
        private List <Row> modelRows;

        private OnlineTrainMapPartFunc(String dataSchemaJson, AlinkParameter params) {
            this.dataSchemaJson = dataSchemaJson;
            this.params = params;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.modelRows = getRuntimeContext(). <Row>getBroadcastVariable("InitModelRows");
            if (params.contains(AlinkPredictor.CLASS_CANONICAL_NAME)) {
                String trainerClassName = params.getString(AlinkPredictor.CLASS_CANONICAL_NAME);
                if (trainerClassName.startsWith("com.alibaba.alink")) {
                    Class trainerClass = Class.forName(trainerClassName);
                    this.trainer = (SingleOnlineTrainer) trainerClass
                            .getConstructor(List.class, TableSchema.class, AlinkParameter.class)
                            .newInstance(modelRows, TableUtil.fromSchemaJson(dataSchemaJson), params);
                }
            } else {
                throw new RuntimeException("Params not contain " + AlinkPredictor.CLASS_CANONICAL_NAME);
            }
        }

        @Override
        public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
            this.trainer.train(values);
            LinearModel linearModel = new LinearModel();
            linearModel.load(this.modelRows);
            linearModel.setCoefInfo(this.trainer.getCoefInfo());
            List <Row> rows = linearModel.save();
            for (Row row : rows) {
                out.collect(row);
            }
        }
    }


    public static class CreateInitModel implements MapPartitionFunction <Row, Row> {
        private String modelName;
        private LinearModelType modelType;
        private TypeInformation <?> labelType;
        private boolean hasInterceptItem;
        private boolean isSparse;
        private boolean isRegProc;
        private String positiveLabelValueString;
        private String[] featureColNames;

        private CreateInitModel(String modelName, LinearModelType modelType, TypeInformation <?> labelType,
                                boolean hasInterceptItem, boolean isSparse, boolean isRegProc,
                                String positiveLabelValueString, String[] featureColNames) {
            this.modelName = modelName;
            this.modelType = modelType;
            this.labelType = labelType;
            this.hasInterceptItem = hasInterceptItem;
            this.isSparse = isSparse;
            this.isRegProc = isRegProc;
            this.positiveLabelValueString = positiveLabelValueString;
            this.featureColNames = featureColNames;
        }

        @Override
        public void mapPartition(Iterable <Row> rows, Collector <Row> collector) throws Exception {
            Object[] labels = null;
            if (!this.isRegProc) {
                labels = orderLabels(this.positiveLabelValueString, rows);
            }

            AlinkParameter meta = new AlinkParameter();
            meta.put(ParamName.modelName, this.modelName, String.class);
            meta.put(ParamName.linearModelType, this.modelType, LinearModelType.class);
            meta.put(ParamName.labelValues, labels);

            meta.put(ParamName.modelName, MLModelType.LinearModel, MLModelType.class);

            meta.put(ParamName.labelType, JdbcTypeConverter.getSqlType(this.labelType), String.class);
            meta.put(ParamName.hasInterceptItem, this.hasInterceptItem, Boolean.class);
            if (this.isSparse) {
                meta.put(ParamName.isSparse, this.isSparse);
            }

            int nWeight = this.hasInterceptItem ? this.featureColNames.length + 1 : this.featureColNames.length;
            List <Row> modelRows = new LinearModel(meta, this.featureColNames, new DenseVector(nWeight)).save();
            for (Row row : modelRows) {
                collector.collect(row);
            }
        }
    }

    /**
     * if has positiveLabelValueString
     * then let the positivaLabelValue be the first element of labels array,
     * else order by the dictionary order
     *
     * @param positiveLabelValueString
     * @param unorderedLabelRows
     * @return
     */
    public static Object[] orderLabels(String positiveLabelValueString, Iterable <Row> unorderedLabelRows) {
        List <Object> tmpArr = new ArrayList <>();
        for (Row row : unorderedLabelRows) {
            tmpArr.add(row.getField(0));
        }
        Object[] labels = tmpArr.toArray(new Object[0]);

        if (null == positiveLabelValueString) {
            String str0 = labels[0].toString();
            String str1 = labels[1].toString();
            positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;
        }

        if (labels[1].toString().equals(positiveLabelValueString)) {
            Object t = labels[0];
            labels[0] = labels[1];
            labels[1] = t;
        }

        return labels;
    }

}