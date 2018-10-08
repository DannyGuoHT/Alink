package com.alibaba.alink.common.ml;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkModel;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.DenseVector;
import com.alibaba.alink.common.matrix.SparseVector;
import com.alibaba.alink.common.matrix.Tensor;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.io.utils.JdbcTypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.AlinkSession.gson;

public class LinearModel extends MLModel {

    String modelName;
    String[] featureNames;
    public DenseVector coefVector = null;
    public DenseVector[] coefVectors = null;
    Object[] labelValues = null;
    TypeInformation<?> labelType = null;
    LinearModelType linearModelType;
    boolean hasInterceptItem = true;
    boolean isSparse = false;
    public int sparseFeatureDim;

    public boolean isMultiClass = false;
    public String multiClassType;
    public int[][] indicesForMxM = null;
    public int[] splitPoints = null;

    public LinearModel() {

    }

    public LinearModel(AlinkParameter meta, String[] featureNames, DenseVector coefVector) {
        this.coefVector = coefVector;
        this.featureNames = featureNames;
        setMetaInfo(meta);
    }

    public LinearModel(AlinkParameter meta, String[] featureNames, List<Tuple2<Integer, DenseVector>> coefVectors) {
        this.coefVectors = new DenseVector[coefVectors.size()];
        for (Tuple2<Integer, DenseVector> t2 : coefVectors) {
            this.coefVectors[t2.f0] = t2.f1;
            System.out.println(t2.f0 + " " + t2.f1);
        }
        this.featureNames = featureNames;
        setMetaInfo(meta);
    }

    public boolean hasInterceptItem() {
        return hasInterceptItem;
    }

    public boolean isSparse() {
        return isSparse;
    }

    public AlinkParameter getMetaInfo() {
        AlinkParameter meta = new AlinkParameter();
        meta.put(ParamName.modelName, modelName);
        meta.put(ParamName.labelValues, labelValues);
        meta.put(ParamName.labelType, JdbcTypeConverter.getSqlType(labelType), String.class);
        meta.put(ParamName.modelType, MLModelType.LinearModel, MLModelType.class);
        meta.put(ParamName.hasInterceptItem, hasInterceptItem);
        meta.put(ParamName.isMultiClass, this.isMultiClass);
        meta.put(ParamName.multiClassType, this.multiClassType);
        meta.put(ParamName.linearModelType, linearModelType, LinearModelType.class);

        if (multiClassType.equals("ManyVsMany")) {
            meta.put("IndicesForMxM", this.indicesForMxM, int[][].class);
            meta.put("SplitPoints", this.splitPoints, int[].class);
        }

        meta.put(ParamName.isSparse, isSparse);
        meta.put(ParamName.sparseFeatureDim, sparseFeatureDim);

        return meta;
    }

    public void setMetaInfo(AlinkParameter meta) {
        this.modelName = meta.getString(ParamName.modelName);
        this.linearModelType = meta.get(ParamName.linearModelType, LinearModelType.class);
        this.isMultiClass = meta.getBoolOrDefault(ParamName.isMultiClass, false);
        this.multiClassType = meta.getStringOrDefault(ParamName.multiClassType, "");
        if (meta.contains(ParamName.hasInterceptItem))
            this.hasInterceptItem = meta.getBool(ParamName.hasInterceptItem);

        if (meta.contains(ParamName.isSparse)) {
            this.isSparse = meta.getBool(ParamName.isSparse);
            this.sparseFeatureDim = meta.getInteger(ParamName.sparseFeatureDim);
        }
        if (this.multiClassType.equals("ManyVsMany")) {
            this.indicesForMxM = meta.get("IndicesForMxM", int[][].class);
            this.splitPoints = meta.get("SplitPoints", int[].class);
        }

        Tuple2<TypeInformation<?>, Object[]> t2 = getLableTypeValues(meta);
        this.labelType = t2.f0;
        this.labelValues = t2.f1;
    }


    public String[] getFeatureNames() {
        return featureNames;
    }

    public void setFeatureNames(String[] featureNames) {
        this.featureNames = featureNames;
    }

    public Object[] getlabelValues() {
        return labelValues;
    }

    public void setlabelValues(Object[] labelValues) {
        this.labelValues = labelValues;
    }

    public DenseVector[] getCoefVectors() {
        return coefVectors;
    }

    public DenseVector getCoefVector() {
        return coefVector;
    }

    public void setCoefVector(DenseVector coefVector) {
        this.coefVector = coefVector;
    }

    public TypeInformation<?> getlabelType() {
        return labelType;
    }

    public void setlabelType(TypeInformation<?> labelType) {
        this.labelType = labelType;
    }

    public LinearModelType getLinearModelType() {
        return linearModelType;
    }


    public LinearCoefInfo getCoefInfo() {
        LinearCoefInfo coefInfo = new LinearCoefInfo();
        coefInfo.featureColNames = this.featureNames;
        coefInfo.coefVector = this.coefVector;
        coefInfo.coefVectors = this.coefVectors;
        return coefInfo;
    }

    public void setCoefInfo(LinearCoefInfo coefInfo) {
        this.featureNames = coefInfo.featureColNames;
        this.coefVector = coefInfo.coefVector;
        this.coefVectors = coefInfo.coefVectors;
    }

    @Override
    public void load(List<Row> rows) {
        AlinkParameter meta = AlinkModel.getMetaParams(rows);
        setMetaInfo(meta);
        String json = AlinkModel.extractStringFromModel(rows);
        setCoefInfo(gson.fromJson(json, LinearCoefInfo.class));
    }

    @Override
    public List<Row> save() {
        ArrayList<Row> list = new ArrayList<>();
        list.add(Row.of(new Object[]{0L, getMetaInfo().toJson(), "", ""}));
        AlinkModel.appendStringToModel(gson.toJson(getCoefInfo()), list);
        return list;
    }

    public static Vector getFeatureVector(Row row, boolean isSparse, boolean hasInterceptItem, int featureN, int[] featureIdx,
                                          int tensorColIndex, int sparseFeatureDim) {
        Vector aVector = null;
        if (tensorColIndex != -1) {
            if (isSparse) {
                String tensor = (String) row.getField(tensorColIndex);
                SparseVector tmp = Tensor.parse(tensor).toSparseVector();
                tmp.setSize(sparseFeatureDim);
                aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
            } else {
                String tensor = (String) row.getField(tensorColIndex);
                DenseVector tmp = Tensor.parse(tensor).toDenseVector();
                aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
            }
        } else {
            if (hasInterceptItem) {
                aVector = new DenseVector(featureN + 1);
                aVector.set(0, 1.0);
                for (int i = 0; i < featureN; i++) {
                    if (row.getField(featureIdx[i]) instanceof Number) {
                        aVector.set(i + 1, ((Number) row.getField(featureIdx[i])).doubleValue());
                    }
                }
            } else {
                aVector = new DenseVector(featureN);
                for (int i = 0; i < featureN; i++) {
                    if (row.getField(featureIdx[i]) instanceof Number) {
                        aVector.set(i, ((Number) row.getField(featureIdx[i])).doubleValue());
                    }
                }
            }
        }
        return aVector;
    }

    public static double getLabelValue(Row row, boolean isRegProc, int labelColIndex, String positiveLableValueString) {
        if (isRegProc) {
            return ((Number) row.getField(labelColIndex)).doubleValue();
        } else {
            return getLabelValue(row.getField(labelColIndex).toString(), positiveLableValueString);
        }
    }

    public static double getLabelValue(String labelValueString, String positiveLableValueString) {
        return labelValueString.equals(positiveLableValueString) ? 1.0 : -1.0;
    }

    public static double getWeightValue(Row row, int weightColIndex) {
        return weightColIndex >= 0 ? ((Number) row.getField(weightColIndex)).doubleValue() : 1.0;
    }

}
