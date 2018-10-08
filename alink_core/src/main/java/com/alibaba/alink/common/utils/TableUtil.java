package com.alibaba.alink.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.alibaba.alink.io.utils.CsvUtil;
import com.alibaba.alink.io.utils.JdbcTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import static com.alibaba.alink.common.AlinkSession.gson;

public class TableUtil {
    public synchronized static String getTempTableName() {
        return ("temp_" + UUID.randomUUID().toString().replaceAll("-", "_"))
            .toLowerCase();//temp table name must be lower case
    }

    public static void assertSameSchema(TableSchema schema1, TableSchema schema2, String errMsg) {
        if (null == schema1 || null == schema2) {
            if (null == schema1 && null == schema2) {
                return;
            } else {
                throw new RuntimeException(errMsg);
            }
        }
        if (null == schema1.getColumnNames() || null == schema1.getTypes() || null == schema2.getColumnNames()
            || null == schema2.getTypes()) {
            throw new RuntimeException(errMsg);
        }
        int n = schema1.getColumnNames().length;
        if (schema1.getTypes().length != n || schema2.getColumnNames().length != n || schema2.getTypes().length != n) {
            throw new RuntimeException(errMsg);
        }
        for (int i = 0; i < n; i++) {
            if (!schema1.getColumnNames()[i].equals(schema2.getColumnNames()[i])) {
                throw new RuntimeException(errMsg);
            }
        }
        for (int i = 0; i < n; i++) {
            if (!schema1.getTypes()[i].equals(schema2.getTypes()[i])) {
                throw new RuntimeException(errMsg);
            }
        }
    }

    public static int findIndexFromName(String[] colNames, String name) {
        int idx = -1;
        for (int i = 0; i < colNames.length; i++) {
            if (name.compareToIgnoreCase(colNames[i]) == 0) {
                idx = i;
            }
        }
        return idx;
    }

    public static String[] getNumericColNames(TableSchema schema) {
        List <String> excludeColNames = null;
        return getNumericColNames(schema, excludeColNames);
    }

    public static String[] getStringColNames(TableSchema schema) {
        String[] inColNames = schema.getColumnNames();
        ArrayList <String> selectedColNames = new ArrayList <>();
        TypeInformation <?>[] inColTypes = schema.getTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (inColTypes[i] == Types.STRING) {
                selectedColNames.add(inColNames[i]);
            }
        }
        return selectedColNames.toArray(new String[0]);
    }

    public static String[] getBooleanColNames(TableSchema schema) {
        String[] inColNames = schema.getColumnNames();
        ArrayList <String> selectedColNames = new ArrayList <>();
        TypeInformation <?>[] inColTypes = schema.getTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (inColTypes[i] == Types.BOOLEAN) {
                selectedColNames.add(inColNames[i]);
            }
        }
        return selectedColNames.toArray(new String[0]);
    }

    public static String[] getNumericColNames(TableSchema schema, String[] excludeColNames) {
        return getNumericColNames(schema, (null == excludeColNames) ? null : Arrays.asList(excludeColNames));
    }

    public static String[] getNumericColNames(TableSchema schema, List <String> excludeColNames) {
        HashSet <TypeInformation <?>> hashSet = new HashSet <>();
        hashSet.add(Types.INT);
        hashSet.add(Types.DOUBLE);
        hashSet.add(Types.LONG);
        hashSet.add(Types.FLOAT);
        hashSet.add(Types.SHORT);
        hashSet.add(Types.BYTE);

        ArrayList <String> selectedColNames = new ArrayList <>();
        String[] inColNames = schema.getColumnNames();
        TypeInformation <?>[] inColTypes = schema.getTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (hashSet.contains(inColTypes[i]) && (null == excludeColNames || !excludeColNames.contains(
                inColNames[i]))) {
                selectedColNames.add(inColNames[i]);
            }
        }

        return selectedColNames.toArray(new String[0]);
    }

    public static String toSchemaJson(TableSchema schema) {
        TypeInformation <?>[] types = schema.getTypes();
        int n = types.length;
        String[] typestrs = new String[n];
        for (int i = 0; i < n; i++) {
            typestrs[i] = JdbcTypeConverter.getSqlType(types[i]);
        }
        return gson.toJson(new String[][] {schema.getColumnNames(), typestrs}, String[][].class);
    }

    public static TableSchema fromSchemaJson(String schemaJson) {
        String[][] t = gson.fromJson(schemaJson, String[][].class);
        int n = t[1].length;
        TypeInformation <?>[] types = new TypeInformation <?>[n];
        for (int i = 0; i < n; i++) {
            types[i] = JdbcTypeConverter.getFlinkType(t[1][i]);
        }
        return new TableSchema(t[0], types);
    }

    public static TableSchema schemaStr2Schema(String schemaStr) {
        String[] fields = schemaStr.split(",");
        String[] colNames = new String[fields.length];
        TypeInformation[] colTypes = new TypeInformation[fields.length];
        for (int i = 0; i < colNames.length; i++) {
            String[] kv = fields[i].trim().split("\\s+");
            colNames[i] = kv[0];
            colTypes[i] = CsvUtil.stringToType(kv[1]);
        }
        return new TableSchema(colNames, colTypes);
    }

    public static String schema2SchemaStr(TableSchema schema) {
        String[] colNames = schema.getColumnNames();
        TypeInformation[] colTypes = schema.getTypes();

        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < colNames.length; i++) {
            if (i > 0) {
                sbd.append(",");
            }
            sbd.append(colNames[i]).append(" ").append(CsvUtil.typeToString(colTypes[i]));
        }
        return sbd.toString();
    }

    static TypeInformation getColType(TypeInformation[] types, String[] names, String name) {
        int index = getColIndex(names, name);

        if (index == -1) {
            return null;
        }

        return types[index];
    }

    public static int getColIndex(String[] names, String name) {
        int index = -1;
        int len = names.length;

        for (int i = 0; i < len; ++i) {
            if (names[i].equals(name)) {
                index = i;
                break;
            }
        }

        return index;
    }

    public static TypeInformation <?>[] getColTypes(
        TypeInformation <?>[] allTypes, String[] allNames, String[] selected) {
        if (selected == null) {
            return null;
        }

        TypeInformation <?>[] ret = new TypeInformation[selected.length];

        for (int i = 0; i < selected.length; ++i) {
            ret[i] = getColType(allTypes, allNames, selected[i]);
        }

        return ret;
    }
}
