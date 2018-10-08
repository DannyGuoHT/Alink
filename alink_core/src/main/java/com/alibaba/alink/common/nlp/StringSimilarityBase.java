package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

/**
 * Calculate the similarity between two targets, by character or by word.
 */

public class StringSimilarityBase implements Serializable {
    private StringSimilarityConst.Method method;
    private Boolean text;
    private Similarity similarity;

    /**
     * Set the parameters for different calculate method.
     */
    public StringSimilarityBase(AlinkParameter params, Boolean text){
        this.method = params.getOrDefault("method", StringSimilarityConst.Method.class, StringSimilarityConst.METHOD);
        this.text = text;
        switch (method){
            // levenshtein supports distance and similarity.
            case LEVENSHTEIN: case LEVENSHTEIN_SIM: {
                this.similarity = new LevenshteinDistance();
                break;
            }
            // lcs supports distance and similarity.
            case LCS: case LCS_SIM: {
                this.similarity = new LongestCommonSubsequence();
                break;
            }
            // ssk supports only similarity.
            case SSK: {
                Integer k = params.getIntegerOrDefault("k", StringSimilarityConst.K);
                Double lambda = params.getDoubleOrDefault("lambda", StringSimilarityConst.LAMBDA);
                this.similarity = new SubsequenceKernel(k, lambda);
                break;
            }
            // cosine supports only similarity.
            case COSINE: {
                Integer k = params.getIntegerOrDefault("k", StringSimilarityConst.K);
                this.similarity = new Cosine(k);
                break;
            }
            // simHash hamming supports distance and similarity.
            case SIMHASH_HAMMING: case SIMHASH_HAMMING_SIM: {
                Integer k = params.getIntegerOrDefault("k", StringSimilarityConst.K);
                this.similarity = new SimHashHamming(k);
                break;

            }
            // minHash supports minHash similarity and jaccard similarity.
            case MINHASH_SIM: case JACCARD_SIM: {
                Integer seed = params.getIntegerOrDefault("seed", StringSimilarityConst.SEED);
                Integer minHashK = params.getIntegerOrDefault("minHashK", StringSimilarityConst.MIN_HASH_K);
                Integer bucket = params.getIntegerOrDefault("bucket", StringSimilarityConst.BUCKET);
                this.similarity = new MinHash(seed, minHashK, bucket);
                break;
            }
            default:{
                throw new RuntimeException("No such similarity method");
            }
        }
    }

    public Double stringSimilairtyRes(Row row) {
        double d;
        // Get the two targets.
        String d1 = row.getField(0).toString();
        String d2 = row.getField(1).toString();
        Vector<String> v1 = new Vector<>(Arrays.asList(d1.split(" ", -1)));
        Vector<String> v2 = new Vector<>(Arrays.asList(d2.split(" ", -1)));

        switch (method) {
            case LEVENSHTEIN: case LCS: case SIMHASH_HAMMING: case JACCARD_SIM: {
                if (text) {
                    // Calculate the similarity by character.
                    d = this.similarity.distance(v1, v2);
                } else {
                    // Calculate the similarity by word.
                    d = this.similarity.distance(d1, d2);
                }
                break;
            }
            case LEVENSHTEIN_SIM: case LCS_SIM: case COSINE: case SSK: case SIMHASH_HAMMING_SIM: case MINHASH_SIM:{
                if (text) {
                    d = this.similarity.similarity(v1, v2);
                } else {
                    d = this.similarity.similarity(d1, d2);
                }
                break;
            }
            default:{
                throw new RuntimeException("No such similarity method");
            }
        }
        return d;
    }
}
