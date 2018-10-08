package com.alibaba.alink.common.utils;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.io.JdbcDB;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

public abstract class AlinkModel {

    protected AlinkParameter meta = new AlinkParameter();

    public final static String SCHEMA_PREFIX1 = "a1";
    public final static String SCHEMA_PREFIX2 = "a2";
    public static final TableSchema DEFAULT_MODEL_SCHEMA = new TableSchema(
        new String[] {"alinkmodelid", "alinkmodelinfo", SCHEMA_PREFIX1, SCHEMA_PREFIX2},
        new TypeInformation <?>[] {Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING()}
    );

    public AlinkParameter getMeta() {
        return this.meta;
    }

    public static TableSchema getModelSchemaWithInfo(ModelType modelType, String info) {
        String modelTypeColName = (SCHEMA_PREFIX1 + modelType.getValue()).toLowerCase();
        String infoColName = (SCHEMA_PREFIX2 + (info == null ? "" : info)).toLowerCase();

        String[] defaultColNames = DEFAULT_MODEL_SCHEMA.getColumnNames();
        int len = defaultColNames.length;

        String[] infoColNames = Arrays.copyOf(defaultColNames, len);

        infoColNames[len - 2] = modelTypeColName;
        infoColNames[len - 1] = infoColName;

        return new TableSchema(infoColNames, DEFAULT_MODEL_SCHEMA.getTypes());
    }

    public static Tuple2 <ModelType, String> getInfoWithModelSchema(TableSchema modelSchema) {
        String[] colNames = modelSchema.getColumnNames();

        if (colNames == null) {
            throw new RuntimeException("Table is not a alink model.");
        }

        int len = colNames.length;
        String modelTypeColName = null;
        String infoColName = null;

        for (int i = 0; i < len; ++i) {
            if (colNames[i].startsWith(SCHEMA_PREFIX1)) {
                if (modelTypeColName != null) {
                    throw new RuntimeException("It is not a alink model. colNames: "
                        + ArrayUtil.array2str(colNames, ","));
                }
                modelTypeColName = colNames[i];
            } else if (colNames[i].startsWith(SCHEMA_PREFIX2)) {
                if (infoColName != null) {
                    throw new RuntimeException("It is not a alink model. colNames: "
                        + ArrayUtil.array2str(colNames, ","));
                }
                infoColName = colNames[i];
            }
        }

        if (modelTypeColName == null
            || infoColName == null) {
            throw new RuntimeException("It is not a alink model. model type column name: " + modelTypeColName
                + ". Infomation column name: " + infoColName + ".");
        }

        String modelType = modelTypeColName.substring(SCHEMA_PREFIX1.length()).trim().toLowerCase();
        String info = infoColName.substring(SCHEMA_PREFIX2.length()).trim().toLowerCase();

        Tuple2 <ModelType, String> ret = new Tuple2 <>();
        if (modelType.equals(ModelType.ML.getValue())) {
            ret.f0 = ModelType.ML;
        } else if (modelType.equals(ModelType.OTHERS.getValue())) {
            ret.f0 = ModelType.OTHERS;
        } else {
            throw new RuntimeException("Unsupported model type. type: " + modelType);
        }
        ret.f1 = info;

        return ret;
    }

    public abstract void load(List <Row> rows);

    public abstract List <Row> save();

    public static void assertModelSchema(TableSchema schema) {
        assertModelSchema(schema, "This is not an Alink Model!");
    }

    public static void assertModelSchema(TableSchema modelSchema, String errMsg) {
        if (null == modelSchema) {
            throw new RuntimeException(errMsg);
        }
        if (null == modelSchema.getColumnNames() || null == modelSchema.getTypes()) {
            throw new RuntimeException(errMsg);
        }
        int n = DEFAULT_MODEL_SCHEMA.getColumnNames().length;
        if (modelSchema.getTypes().length != n || modelSchema.getColumnNames().length != n) {
            throw new RuntimeException(errMsg);
        }
        String[] defaultColNames = DEFAULT_MODEL_SCHEMA.getColumnNames();
        String[] colNames = modelSchema.getColumnNames();
        if (!colNames[0].equals(defaultColNames[0]) || !colNames[1].equals(defaultColNames[1]) ||
            !colNames[2].startsWith(SCHEMA_PREFIX1) || !colNames[3].startsWith(SCHEMA_PREFIX2)) {
            throw new RuntimeException(errMsg);
        }
        for (int i = 0; i < n; i++) {
            if (!modelSchema.getTypes()[i].equals(DEFAULT_MODEL_SCHEMA.getTypes()[i])) {
                throw new RuntimeException(errMsg);
            }
        }
    }

    public static Row getMetaRow(List <Row> rows) {
        for (Row row : rows) {
            if (row.getField(0).equals(new Long(0))) {
                if (row.getArity() == DEFAULT_MODEL_SCHEMA.getColumnNames().length) {
                    return row;
                }
            }
        }
        throw new RuntimeException("There is no meta row, or this is not an ALink Model");
    }

    public static AlinkParameter getMetaParams(List <Row> rows) {
        return getMetaParams(getMetaRow(rows));
    }

    public static AlinkParameter getMetaParams(Row row) {
        if (row.getField(0).equals(new Long(0))) {
            if (row.getArity() == DEFAULT_MODEL_SCHEMA.getColumnNames().length) {
                return AlinkParameter.fromJson(row.getField(1).toString());
            }
        }
        throw new RuntimeException("There is no meta row, or this is not an ALink Model");
    }

    public static int appendStringToModel(String json, List <Row> rows) {
        return appendStringToModel(json, rows, 1);
    }

    public static int appendStringToModel(String json, List <Row> rows, int startIndex) {
        return appendStringToModel(json, rows, startIndex, JdbcDB.MAX_VARCHAR_SIZE);
    }

    public static int appendStringToModel(String json, List <Row> rows, int startIndex, int unitSize) {
        if (null == json || json.length() == 0) {
            return startIndex;
        }
        int n = json.length();
        int cur = 0;
        int rowIndex = startIndex;
        while (cur < n) {
            String str = json.substring(cur, Math.min(cur + unitSize, n));
            Row row = new Row(4);
            row.setField(0, Long.valueOf(rowIndex));
            row.setField(1, str);
            rows.add(row);
            rowIndex++;
            cur += unitSize;
        }
        return rowIndex;
    }

    public static String extractStringFromModel(List <Row> rows) {
        return extractStringFromModel(rows, 1);
    }

    public static String extractStringFromModel(List <Row> rows, int startIndex) {
        int m = rows.size();
        String[] strs = new String[m];
        for (Row row : rows) {
            int idx = ((Long)row.getField(0)).intValue();
            if (idx >= startIndex) {
                strs[idx - startIndex] = (String)row.getField(1);
            }
        }
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < m; i++) {
            if (null == strs[i]) { break; }
            sbd.append(strs[i]);
        }
        return sbd.toString();
    }

    public enum ModelType {
        // ML model
        ML("m"),
        // other model
        OTHERS("o");

        private final String value;

        private ModelType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
