package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Calculate the cosine similarity.
 * Cosine similarity: a measure of similarity between two non-zero vectors of an inner product
 * space that measures the cosine of the angle between them.
 */

public class Cosine implements Serializable, Similarity {
    private int k;

    /**
     * Set the parameter k.
     */
    public Cosine(int k){
        if(k <= 0){
            throw new RuntimeException("k must be positive!");
        }
        this.k = k;
    }

    /**
     * List all the words in the two inputs.
     * Count the word frequency as vectors.
     * Calculate the dot product A·B between the two vectors.
     */
    double calCosine(Vector<String> lsv, Vector<String> rsv) {
        Map<String, int[]> map = new HashMap<>();

        for(int i = 0; i < lsv.size(); i++){
            String key = lsv.get(i);
            if(map.containsKey(key)){
                map.get(key)[0] += 1;
            }else
            {
                int[] tmp = new int[2];
                tmp[0] = 1;
                tmp[1] = 0;
                map.put(key, tmp);
            }
        }
        for(int i = 0; i < rsv.size(); i++){
            String key = rsv.get(i);
            if(map.containsKey(key)){
                map.get(key)[1] += 1;
            }else{
                int[] tmp = new int[2];
                tmp[0] = 0;
                tmp[1] = 1;
                map.put(key, tmp);
            }
        }

        double ret = 0;
        Iterator<Map.Entry<String, int[]>> entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, int[]> entry = entries.next();
            ret += (entry.getValue()[0] * entry.getValue()[1]);
        }
        return ret;
    }

    /**
     * Split the input using a sliding window.
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right) {
        Vector<String> lsv = CommonDistanceFunc.splitWord2V(left,k);
        Vector<String> rsv = CommonDistanceFunc.splitWord2V(right,k);

        return calCosine(lsv, rsv);
    }

    /**
     * Similarity = A·B / (|A| * |B|)
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right) {
        double numerator = distance(left, right);
        double denominator = Math.sqrt(distance(left, left) * distance(right, right));

        denominator = denominator == 0. ? 1e-16 : denominator;
        double ret = numerator / (denominator);
        ret = ret > 1.0 ? 1.0 : ret;
        return ret;
    }
}
