package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class StringSimilarityConst implements Serializable{
    public static final int K = 2;
    public static final double LAMBDA = 0.5;
    public static final Method METHOD = Method.LEVENSHTEIN;
    public static final String OUTPUT = "output";
    public static final int BIT_LENGTH = 64;
    public static final int SEED = 0;
    public static final int MIN_HASH_K = 100;
    public static final int BUCKET = 10;
    public static final int INTERVAL = 3;
    public static final int TOP_N = 10;
    public static final String[] KEEPCOLNAMES = new String[]{};

    public enum Method
    {
        /**
         * <code>Levenshtein distance;</code>
         */
        LEVENSHTEIN,
        /**
         * <code>Levenshtein similarity;</code>
         */
        LEVENSHTEIN_SIM,
        /**
         * <code>Lcs distance;</code>
         */
        LCS,
        /**
         * <code>Lcs similarity;</code>
         */
        LCS_SIM,
        /**
         * <code>ssk similarity;</code>
         */
        SSK,
        /**
         * <code>cosine similarity;</code>
         */
        COSINE,
        /**
         * <code>simhash hamming distance;</code>
         */
        SIMHASH_HAMMING,
        /**
         * <code>simhash hamming similarity;</code>
         */
        SIMHASH_HAMMING_SIM,
        /**
         * <code>minhash similarity;</code>
         */
        MINHASH_SIM,
        /**
         * <code>Jaccard similarity;</code>
         */
        JACCARD_SIM
    }

}
