package com.alibaba.alink.common.nlp;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;

/**
 * Calculate the SimHash Hamming Distance.
 * SimHash Hamming: Hash the inputs to BIT_LENGTH size, and calculate the hamming distance.
 */
public class SimHashHamming implements Serializable,Similarity {
    private int k;

    /**
     * Set the parameter K.
     */
    public SimHashHamming(int k){
        if(k <= 0){
            throw new RuntimeException("k must be positive!");
        }
        this.k = k;
    }

    /**
     * Split the input using a sliding window.
     */
    private <T> BigInteger getInteger(T str) {
        Vector<String> vec = CommonDistanceFunc.splitWord2V(str, k);
        return simHash(vec);
    }

    /**
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right) {
        return hammingDistance(getInteger(left), getInteger(right));
    }

    /**
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right) {
        return hammingSimilarity(getInteger(left), getInteger(right));
    }

    /**
     * Compare the two BigIntegers, and count the number of bit differences.
     */
    double hammingDistance(BigInteger left, BigInteger right) {
        BigInteger m = BigInteger.ONE.shiftLeft(StringSimilarityConst.BIT_LENGTH).subtract(BigInteger.ONE);
        BigInteger x = left.xor(right).and(m);
        int d = 0;
        while (!x.equals(BigInteger.ZERO))
        {
            d += 1;
            x = x.and(x.subtract(BigInteger.ONE));
        }
        return d;
    }

    /**
     * Similarity = 1.0 - distance / BIT_LENGTH.
     */
    double hammingSimilarity(BigInteger left, BigInteger right) {
        double d = hammingDistance(left, right);

        d = 1 - d / StringSimilarityConst.BIT_LENGTH;
        return d;
    }

    /**
     * Hash the vector to BIT_LENGTH size and add the weights.
     */
    BigInteger simHash(Vector<String> words) {
        // Count the word frequency.
        Map<String, Double> keywords = new HashMap<>();
        for(int i = 0; i < words.size(); i++){
            String key = words.get(i);
            if(keywords.containsKey(key)){
                keywords.put(key, keywords.get(key) + 1);
            }else{
                keywords.put(key, 1.0);
            }
        }

        Vector<Double> weights = new Vector<>(StringSimilarityConst.BIT_LENGTH);
        for(int i = 0; i < StringSimilarityConst.BIT_LENGTH; i++){
            weights.add(0.0);
        }

        Iterator<Map.Entry<String, Double>> entries = keywords.entrySet().iterator();
        try{
            while (entries.hasNext()) {
                Map.Entry<String, Double> entry = entries.next();
                // Hash each word to BIT_LENGTH size using MD5.
                BigInteger t = new HashFuncMD5(StringEscapeUtils.unescapeJava(entry.getKey())).hexdigest();
                for(int i = 0; i < StringSimilarityConst.BIT_LENGTH; i++){
                    BigInteger mask = BigInteger.ONE.shiftLeft(i);
                    // With word frequency as weight, accumulate the weighted BITs.
                    weights.set(i, weights.get(i) + (mask.and(t).equals(BigInteger.ZERO) ? -1 : 1) * entry.getValue());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        BigInteger res = BigInteger.ZERO;

        // reduce the dimension to 0-1 sequence
        for(int i = 0; i < StringSimilarityConst.BIT_LENGTH; i++){
            if(weights.get(i) > 0.0){
                res = res.or((BigInteger.ONE.shiftLeft(i)));
            }
        }
        return res;
    }
}
