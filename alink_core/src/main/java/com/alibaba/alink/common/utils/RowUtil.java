package com.alibaba.alink.common.utils;

import org.apache.flink.types.Row;

public class RowUtil {
    public static Row remove(Row rec, int idx) {
        int n1 = rec.getArity();
        Row ret = new Row(n1 - 1);
        for (int i = 0; i < n1; ++i) {
            if (i < idx) {
                ret.setField(i, rec.getField(i));
            } else if (i > idx) {
                ret.setField(i - 1, rec.getField(i));
            }
        }
        return ret;
    }

    public static Row merge(Row rec1, Object obj) {
        int n1 = rec1.getArity();
        Row ret = new Row(n1 + 1);
        for (int i = 0; i < n1; ++i) {
            ret.setField(i, rec1.getField(i));
        }
        ret.setField(n1, obj);
        return ret;
    }

    public static Row merge(Object obj, Row rec1) {
        int n1 = rec1.getArity();
        Row ret = new Row(n1 + 1);
        ret.setField(0, obj);
        for (int i = 0; i < n1; ++i) {
            ret.setField(i + 1, rec1.getField(i));
        }
        return ret;
    }

    public static Row merge(Row rec1, Row rec2) {
        int n1 = rec1.getArity();
        int n2 = rec2.getArity();
        Row ret = new Row(n1 + n2);
        for (int i = 0; i < n1; ++i) {
            ret.setField(i, rec1.getField(i));
        }
        for (int i = 0; i < n2; ++i) {
            ret.setField(i + n1, rec2.getField(i));
        }
        return ret;
    }

    public static double euclideanDist(Row r1, Row r2) {
        double d = 0.0;
        double t;
        for (int i = 0; i < r1.getArity(); i++) {
            t = (Double)(r1.getField(i)) - (Double)(r2.getField(i));
            d += t * t;
        }
        return Math.sqrt(d);
    }

    public static Row addRow(Row r1, Row r2) {
        Row r = new Row(r1.getArity());
        for (int i = 0; i < r1.getArity(); i++) {
            r.setField(i, (Double)(r1.getField(i)) + (Double)(r2.getField(i)));
        }
        return r;
    }

    public static Row divRow(Row r1, long n) {
        Row r = new Row(r1.getArity());
        for (int i = 0; i < r1.getArity(); i++) {
            r.setField(i, (Double)(r1.getField(i)) / n);
        }
        return r;
    }

    public static String row2Str(Row val, String delimiter) {
        int arity = val.getArity();
        StringBuilder sbd = new StringBuilder();

        for (int i = 0; i < arity; ++i) {
            if (i != 0 && delimiter != null) {
                sbd.append(delimiter);
            }

            sbd.append(val.getField(i).toString());
        }

        return sbd.toString();
    }
}
