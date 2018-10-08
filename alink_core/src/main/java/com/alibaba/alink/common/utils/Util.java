package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class Util {

    /**
     * if feaure
     *
     * @param featureColNames
     * @param tensorColName
     * @return
     */
    public static boolean isTensor(String[] featureColNames, String tensorColName) {
        if (featureColNames != null && featureColNames.length != 0) {
            return false;
        } else if (tensorColName != null) {
            return true;
        } else {
            throw new RuntimeException("feature col or tensor must exist.");
        }
    }

    public static int[] getTensorColIdxs(String[] featureColNames, String tensorColName,
                                         String[] colNames, TypeInformation <?>[] colTypes) {
        int[] featureIdxs = null;
        int featureLength = 1;
        boolean isTensor = isTensor(featureColNames, tensorColName);
        if (!isTensor) {
            featureLength = featureColNames.length;
            featureIdxs = new int[featureLength];
            for (int i = 0; i < featureLength; ++i) {
                featureIdxs[i] = TableUtil.findIndexFromName(colNames, featureColNames[i]);
                if (featureIdxs[i] < 0) {
                    throw new RuntimeException("col name not exist! " + featureColNames[i]);
                }
                if (colTypes[featureIdxs[i]] != Types.DOUBLE) {
                    throw new RuntimeException("col type must be double when table! "
                        + featureColNames[i] + " " + colTypes[featureIdxs[i]]);
                }
            }
        } else {
            featureIdxs = new int[1];
            featureIdxs[0] = TableUtil.findIndexFromName(colNames, tensorColName);
            if (featureIdxs[0] < 0) {
                throw new RuntimeException("col name not exist! " + tensorColName);
            }
            if (colTypes[featureIdxs[0]] != Types.STRING) {
                throw new RuntimeException("col type must be string when tensor! " + tensorColName);
            }
        }
        return featureIdxs;
    }

    public static double getDoubleValue(Object obj, Class type) {
        if (Double.class == type) {
            return ((Double)obj).doubleValue();
        } else if (Integer.class == type) {
            return ((Integer)obj).doubleValue();
        } else if (Long.class == type) {
            return ((Long)obj).doubleValue();
        } else if (Float.class == type) {
            return ((Float)obj).doubleValue();
        } else if (Boolean.class == type) {
            return (Boolean)obj ? 1.0 : 0.0;
        } else if (java.sql.Date.class == type) {
            return (double)((java.sql.Date)obj).getTime();
        } else {
            System.out.println("type: " + type.getName());
            throw new RuntimeException("Not implemented yet!");
        }
    }

    public static double getDoubleValue(Object obj) {
        return getDoubleValue(obj, obj.getClass());
    }
}
