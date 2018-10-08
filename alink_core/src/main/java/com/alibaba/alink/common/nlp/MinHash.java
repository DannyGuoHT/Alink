package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.*;

/**
 * Calculate the MinHash similarity and Jaccard Similarity.
*/

public class MinHash implements Serializable, Similarity {
    private int k,  r;
    private Vector<Integer> coefficientA = new Vector<>();
    private Vector<Integer> coefficientB = new Vector<>();

    /**
     * Hash Function: h(x) = A * x + B.
     * @param seed: seed for generate the random efficients of hash functions.
     * @param k: k independent hash functions.
     * @param b: b bands for LSH.
     */
    public MinHash(int seed, int k, int b){
        this.k = k;
        this.r = k / b;
        if(k % b != 0){
            throw new RuntimeException("k / b != 0. k :" + k + ". b:" + b);
        }
        Random random = new Random(seed);
        coefficientA.setSize(k);
        coefficientB.setSize(k);
        for(int i = 0; i < k; i++){
            coefficientA.set(i, random.nextInt());
            coefficientB.set(i, random.nextInt());
        }
    }

    /**
     * Common Function for jaccard similarity and minHash similarity.
     * jaccard:
     * True: return the jaccard similarity;
     * False: return the minHash similarity.
     */
    private <T> double similarityCommonFunc(T left, T right, boolean jaccard) {
        // Transform the String input to Integer to support hash functions.
        Vector<Integer> leftSorted = CommonDistanceFunc.toInteger(left);
        Vector<Integer> rightSorted = CommonDistanceFunc.toInteger(right);

        // Get k minimum values by applying hash function.
        Vector<Integer> leftMinHash = hashFuncMin(leftSorted);
        Vector<Integer> rightMinHash = hashFuncMin(rightSorted);

        // Apply Locality-sensitive hashing(LSH) algorithm.
        Vector<Integer> leftBuckets = toBucket(leftMinHash);
        Vector<Integer> rightBuckets = toBucket(rightMinHash);

        // Compare the hash values of buckets, if all buckets are different, we think sim is 0.0.
        leftBuckets.retainAll(rightBuckets);
        double sim = 0.0;
        if(!leftBuckets.isEmpty()){
            if(jaccard){
                sim = jaccardSim(leftSorted, rightSorted);
            }else{
                sim = minHashSim(leftMinHash, rightMinHash);
            }
        }
        return sim;
    }

    /**
     * Apply the hash function h(x) = A * x + B to the Vector.
     * Get k minimum values.
     */
    private Vector<Integer> hashFuncMin(Vector<Integer> wordsHash){
        int size = wordsHash.size();
        Vector<Integer> minHashSet  = new Vector<>();
        for(int i = 0; i < k; i++) {
            minHashSet.add(Integer.MAX_VALUE);
        }
        for(int i = 0; i < size; i++) {
            int cur = wordsHash.get(i);
            for (int j = 0; j < k; j++) {
                int curRandWordHash = Math.abs(coefficientA.get(j) * cur + coefficientB.get(j));
                minHashSet.set(j, Math.min(minHashSet.get(j), curRandWordHash));
            }
        }
        return minHashSet;
    }

    /**
     * MinHashSim = P(hmin(A) = hmin(B)) = Count(I(hmin(A) = hmin(B))) / k.
     */
    double minHashSim(Vector<Integer> left, Vector<Integer> right)
    {
        double cnt = 0;
        int size = left.size();
        for (int i = 0; i < size; i++)
        {
            if (left.get(i).equals(right.get(i)))
            {
                cnt += 1.0;
            }
        }
        if (size > 0)
        {
            cnt /= (double)size;
        }
        return cnt;
    }

    /**
     * JaccardSim = |A ∩ B| / |A ∪ B| = |A ∩ B| / (|A| + |B| - |A ∩ B|)
     */
    double jaccardSim(Vector<Integer> left, Vector<Integer> right)
    {
        int i = 0;
        int j = 0;
        double sameCnt = 0;
        int leftSize = left.size();
        int rightSize = right.size();
        if (leftSize == 0 && rightSize == 0)
        {
            return 0.0;
        }
        while(i < leftSize && j < rightSize)
        {
            if (left.get(i).equals(right.get(j)))
            {
                sameCnt += 1.0;
                i += 1;
                j += 1;
            }
            else if (left.get(i) > right.get(j))
            {
                j += 1;
            }
            else
            {
                i += 1;
            }
        }
        return sameCnt / ((double)leftSize + (double)rightSize -  sameCnt);
    }

    /**
     * return the hash value of one bucket.
     */
    private int arrayHash(Vector<Integer> vector, int begin, int end){
        if(begin >= end){
            return 0;
        }
        int ret = 1;
        for(int i = begin; i < end; i++) {
            ret = 31 * ret + vector.get(i);
        }
        return ret;
    }

    /**
     * Apply LSH:
     * For every r hash values, map them to one bucket and calculate the hash value of every bucket.
     */
    private Vector<Integer> toBucket(Vector<Integer> sortedWords) {
        Vector<Integer> buckets = new Vector<>();
        int size = sortedWords.size();
        if (size == 0) {
            return buckets;
        }

        int begin, end;
        int i = 0;
        while(true){
            if(i + r >= size){
                begin = i;
                end = size;
                buckets.add(arrayHash(sortedWords, begin, end));
                return buckets;
            }
            begin = i;
            end = i + r;
            buckets.add(arrayHash(sortedWords, begin, end));
            i += r;
        }
    }

    /**
     * Get the minHash similarity.
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right){
        return similarityCommonFunc(left, right, false);
    }

    /**
     * Get the jaccard similarity.
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right){
        return similarityCommonFunc(left, right, true);
    }
}
