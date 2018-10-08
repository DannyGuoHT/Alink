package com.alibaba.alink.common.ml.clustering;

import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.matrix.Vector;
import com.alibaba.alink.common.matrix.VectorOp;
import com.alibaba.alink.common.matrix.string.StringVector;

public class Distance {

    //KM
    private static int EARTH_RADIUS = 6371;

    /**
     * calc euclidean distance
     *
     * @param point1
     * @param point2
     * @return
     */
    private static double euclideanDistance(Vector point1, Vector point2) {
        return VectorOp.minus(point1, point2).l2norm();
    }

    private static double haverSin(double theta) {
        double value = Math.sin(theta / 2);
        return value * value;
    }

    /**
     * calc haversine distance. latitude,longitude param format in degrees
     *
     * @param point1
     * @param point2
     * @return
     */
    private static double haversineDistance(Vector point1, Vector point2) {
        double lat1 = point1.get(0) * Math.PI / 180;
        double lon1 = point1.get(1) * Math.PI / 180;
        double lat2 = point2.get(0) * Math.PI / 180;
        double lon2 = point1.get(1) * Math.PI / 180;

        double vLon = lon1 - lon2;
        double vLat = lat1 - lat2;
        double h = haverSin(vLat) + Math.cos(lat1) * Math.cos(lat2) * haverSin(vLon);
        return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(h));
    }

    /**
     * cosine distance,[0-1] 0.5-0.5(A*B/(sqrt(A*A)*sqrt(B*B)))
     *
     * @param point1
     * @param point2
     * @return
     */
    private static double cosineDistance(Vector point1, Vector point2) {
        double distance = 0.0;
        double dot = 0.0;
        double cross1 = 0.0;
        double cross2 = 0.0;
        int size = Math.min(point1.size(), point2.size());
        for (int i = 0; i < size; ++i) {
            dot += point1.get(i) * point2.get(i);
            cross1 += Math.pow(point1.get(i), 2);
            cross2 += Math.pow(point2.get(i), 2);
        }
        cross1 = Math.sqrt(cross1);
        cross2 = Math.sqrt(cross2);
        double cross = cross1 * cross2;
        if (cross > 0) {
            distance = -(dot / cross);
            // let distance range [0, 1]
            distance = 0.5 + 0.5 * distance;
        } else {
            distance = 0;
        }
        return distance;
    }

    /**
     * City Block (Manhattan) distance
     *
     * @param point1
     * @param point2
     * @return
     */
    private static double cityBlockDistance(Vector point1, Vector point2) {
        double distance = 0.0;
        int size = Math.min(point1.size(), point2.size());
        for (int i = 0; i < size; i++) {
            distance += Math.abs(point1.get(i) - point2.get(i));
        }
        return distance;
    }

    /**
     * Hamming distance
     *
     * @param point1
     * @param point2
     * @return
     */
    public static int hammingDistance(String point1, String point2) {
        int distance = 0;
        if (null != point1 && null != point2) {
            distance = Math.abs(point1.length() - point2.length());
            int size = Math.min(point1.length(), point2.length());
            for (int i = 0; i < size; i++) {
                if (point1.charAt(i) != point2.charAt(i)) {
                    distance++;
                }
            }
        } else {
            if (!(null == point1 && null == point2)) {
                distance++;
            }
        }
        return distance;
    }

    /**
     * Hamming distance
     *
     * @param a
     * @param b
     * @return
     */
    public static int hammingDistance(Integer a, Integer b) {
        int distance = 0;
        if (null != a && null != b) {
            boolean isHammingCode = (a == 1 || a == 0) && (b == 1 || b == 0);
            if (isHammingCode) {
                distance = (a.equals(b)) ? 0 : 1;
            } else {
                System.out.println("a=" + a + ",b=" + b);
                throw new RuntimeException(
                    "the input " + ParamName.featureColNames + " should be Hamming Code,only support 0 and 1");
            }
        } else {
            if (!(null == a && null == b)) {
                distance++;
            }
        }
        return distance;
    }

    /**
     * Hamming distance for Vector
     *
     * @param point1
     * @param point2
     * @return
     */
    private static int hammingDistance(Vector point1, Vector point2) {
        int size = Math.min(point1.size(), point2.size());
        int distance = 0;
        for (int i = 0; i < size; i++) {
            distance += hammingDistance((int)point1.get(i), (int)point2.get(i));
        }
        return distance;
    }

    /**
     * oneZero Distance
     *
     * @param point1
     * @param point2
     * @return
     */
    private static int oneZeroDistance(String point1, String point2) {
        int distance = 0;
        if (null != point1 && null != point2) {
            if (!point1.equals(point2)) {
                distance++;
            }
        } else {
            if (!(null == point1 && null == point2)) {
                distance++;
            }
        }
        return distance;
    }

    public static double calcDistance(DistanceType distanceType, Vector point1, Vector point2) {
        switch (distanceType) {
            case EUCLIDEAN:
                return euclideanDistance(point1, point2);
            case COSINE:
                return cosineDistance(point1, point2);
            case CITYBLOCK:
                return cityBlockDistance(point1, point2);
            case HAVERSINE:
                return haversineDistance(point1, point2);
            case HAMMING:
                return hammingDistance(point1, point2);
            default:
                throw new RuntimeException("distanceType not support:" + distanceType);
        }
    }

    /**
     * CategoricalDistance
     *
     * @param cDistanceType
     * @param sample1
     * @param sample2
     * @return
     */
    public static int calcCategoricalDistance(CategoricalDistanceType cDistanceType, StringVector sample1,
                                              StringVector sample2) {
        int dim = Math.min(sample1.size(), sample2.size());
        int distance = 0;
        switch (cDistanceType) {
            case ONEZERO:
                for (int i = 0; i < dim; i++) {
                    distance += Distance.oneZeroDistance(sample1.get(i), sample2.get(i));
                }
                break;
            case HAMMING:
                for (int i = 0; i < dim; i++) {
                    distance += Distance.hammingDistance(sample1.get(i), sample2.get(i));
                }
                break;
            default:
                return 0;
        }
        return distance;
    }

}
