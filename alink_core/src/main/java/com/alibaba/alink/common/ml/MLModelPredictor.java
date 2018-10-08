package com.alibaba.alink.common.ml;

import java.util.List;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.utils.AlinkModel.ModelType;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.io.utils.JdbcTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public abstract class MLModelPredictor extends AlinkPredictor {
    public final static String PREDICTION_RESULT_COL_NAME = "prediction_result";

    protected String[] keepColNames;
    protected String predPositiveLabelValueString;
    protected String predScoreColName;
    protected String predDetailColName;
    protected String predResultColName;

    protected int[] keepIdx;
    protected int keepN = -1;

    public MLModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        if (null != params) {
            this.keepColNames = params.getStringArrayOrDefault(ParamName.keepColNames, null);
            this.predResultColName = params.getStringOrDefault(ParamName.predResultColName, null);
            this.predDetailColName = params.getStringOrDefault(ParamName.predDetailColName, null);
            this.predScoreColName = params.getStringOrDefault(ParamName.predScoreColName, null);
            this.predPositiveLabelValueString = params.getStringOrDefault(ParamName.positiveLabelValueString, null);
            if (this.predScoreColName != null && this.predScoreColName.equals("")) {
                this.predScoreColName = null;
            }
            if (this.predDetailColName != null && this.predDetailColName.equals("")) {
                this.predDetailColName = null;
            }
            if (this.predPositiveLabelValueString != null && this.predPositiveLabelValueString.equals("")) {
                this.predPositiveLabelValueString = null;
            }
        }
    }

    @Override
    public TableSchema getResultSchema() {
        int out_size = 1;
        if (this.keepColNames != null) {
            this.keepN = this.keepColNames.length;
            out_size += this.keepN;
            this.keepIdx = new int[this.keepN];
            for (int i = 0; i < keepN; ++i) {
                keepIdx[i] = TableUtil.findIndexFromName(dataSchema.getColumnNames(), keepColNames[i]);
            }
        } else {
            this.keepN = dataSchema.getColumnNames().length;
            out_size += this.keepN;
            this.keepIdx = new int[this.keepN];
            for (int i = 0; i < keepN; ++i) {
                keepIdx[i] = i;
            }
        }

        if (this.predScoreColName != null && this.predPositiveLabelValueString != null) { out_size += 1; }
        if (this.predDetailColName != null) { out_size += 1; }

        String[] outNames = new String[out_size];
        TypeInformation[] outTypes = new TypeInformation[out_size];

        String[] names = dataSchema.getColumnNames();
        TypeInformation[] types = dataSchema.getTypes();

        int iter = 0;
        if (this.keepN != -1) {
            for (int i = 0; i < keepN; i++) {
                outNames[i] = names[keepIdx[i]];
                outTypes[i] = types[keepIdx[i]];
            }

            if (this.predResultColName != null) {
                outNames[this.keepN] = this.predResultColName;
                outTypes[this.keepN] = getTypeWithModelSchema(this.modelScheme);
            } else {
                outNames[this.keepN] = PREDICTION_RESULT_COL_NAME;
                outTypes[this.keepN] = getTypeWithModelSchema(this.modelScheme);
            }
            iter += this.keepN + 1;
        }

        if (this.predDetailColName != null) {
            outNames[iter] = this.predDetailColName;
            outTypes[iter++] = Types.STRING;
        }

        if (this.predScoreColName != null && this.predPositiveLabelValueString != null) {
            outNames[iter] = this.predScoreColName;
            outTypes[iter++] = Types.DOUBLE;
        }

        return new TableSchema(outNames, outTypes);
    }

    public static TypeInformation <?> getTypeWithModelSchema(TableSchema modelSchema) {
        Tuple2 <ModelType, String> t2 = AlinkModel.getInfoWithModelSchema(modelSchema);
        if (ModelType.ML != t2.f0) {
            throw new RuntimeException("This is not a ML Model.");
        } else {
            return JdbcTypeConverter.getFlinkType(t2.f1);
        }
    }

    public static MLModelPredictor of(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params,
                                      List <Row> modelRows) {
        Row metaRow = AlinkModel.getMetaRow(modelRows);
        AlinkParameter meta = MLModel.getMetaParams(metaRow);

        //load model according the model type
        if (null != meta) {
            switch (meta.get("modelType", MLModelType.class)) {
                case LinearModel: {
                    LinearModelPredictor predictor = new LinearModelPredictor(modelScheme, dataSchema, params);
                    predictor.loadModel(modelRows);
                    return predictor;
                }
                case NaiveBayesModel: {
                    NaiveBayesModelPredictor predictor = new NaiveBayesModelPredictor(modelScheme, dataSchema, params);
                    predictor.loadModel(modelRows);
                    return predictor;
                }
                default:
                    throw new RuntimeException("This model is not supported yet!");
            }
        } else {
            throw new RuntimeException("This is not ML model!");
        }

    }
}
