package com.alibaba.alink.common.utils;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class ArrayUtil {

    public static long Max(long[] counts) {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < counts.length; i++) {
            if (max < counts[i]) {
                max = counts[i];
            }
        }
        return max;
    }

    public static long Min(long[] counts) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < counts.length; i++) {
            if (min > counts[i]) {
                min = counts[i];
            }
        }
        return min;
    }

    public static double Max(double[] counts) {
        double max = Double.MIN_VALUE;
        for (int i = 0; i < counts.length; i++) {
            if (max < counts[i]) {
                max = counts[i];
            }
        }
        return max;
    }

    public static double Min(double[] counts) {
        double min = Double.MAX_VALUE;
        for (int i = 0; i < counts.length; i++) {
            if (min > counts[i]) {
                min = counts[i];
            }
        }
        return min;
    }

    public static TypeInformation<?>[] arrayMerge(TypeInformation<?> type1, TypeInformation<?>[] types2) {
        return arrayMerge(new TypeInformation<?>[]{type1}, types2);
    }

    public static TypeInformation<?>[] arrayMerge(TypeInformation<?>[] types1, TypeInformation<?> type2) {
        return arrayMerge(types1, new TypeInformation<?>[]{type2});
    }

    public static TypeInformation<?>[] arrayMerge(TypeInformation<?>[] types1, TypeInformation<?>[] types2) {
        if (null == types1 || null == types2) {
            throw new RuntimeException();
        }
        int n1 = types1.length;
        int n2 = types2.length;
        TypeInformation<?>[] r = new TypeInformation<?>[n1 + n2];
        for (int i = 0; i < n1; i++) {
            r[i] = types1[i];
        }
        for (int i = 0; i < n2; i++) {
            r[n1 + i] = types2[i];
        }
        return r;
    }

    public static String[] arrayMerge(String str1, String[] strs2) {
        return arrayMerge(new String[]{str1}, strs2);
    }

    public static String[] arrayMerge(String[] strs1, String str2) {
        return arrayMerge(strs1, new String[]{str2});
    }

    public static String[] arrayMerge(String[] strs1, String[] strs2) {
        if (null == strs1 || null == strs2) {
            throw new RuntimeException();
        }
        int n1 = strs1.length;
        int n2 = strs2.length;
        String[] r = new String[n1 + n2];
        for (int i = 0; i < n1; i++) {
            r[i] = strs1[i];
        }
        for (int i = 0; i < n2; i++) {
            r[n1 + i] = strs2[i];
        }
        return r;
    }

    public static String[] arrayMergeDistinct(String[] strs1, String[] strs2) {
        if (null == strs1 || null == strs2) {
            throw new RuntimeException();
        }
        Set<String> sets = new HashSet<>();

        for (int i = 0; i < strs1.length; i++) {
            sets.add(strs1[i]);
        }
        for (int i = 0; i < strs2.length; i++) {
            sets.add(strs2[i]);
        }
        return sets.toArray(new String[0]);
    }

    public static <T> String array2str(T[] strs, String delim) {
        if (null == strs) {
            throw new RuntimeException();
        }
        StringBuilder sbd = new StringBuilder();
        sbd.append(strs[0].toString());
        for (int i = 1; i < strs.length; i++) {
            sbd.append(delim).append(strs[i].toString());
        }
        return sbd.toString();
    }
}
