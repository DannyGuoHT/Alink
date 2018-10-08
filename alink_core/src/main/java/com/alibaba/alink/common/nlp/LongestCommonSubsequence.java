package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.Vector;

/**
 * Calculate the LCS.
 * LCS: the longest subsequence common to the two inputs.
 */

public class LongestCommonSubsequence implements Serializable, Similarity {
    private double longestCommonSubsequence(Vector<String> left, Vector<String> right) {
        int lenL = left.size() + 1;
        int lenR = right.size() + 1;
        if(lenL == 0) {
            return lenL;
        }
        if(lenR == 0) {
            return lenR;
        }
        long[][] matrix = new long[lenL][lenR];
        for(int i = 0; i < lenL; i++){
            for(int j = 0; j < lenR; j++){
                if(Math.min(i, j) == 0){
                    matrix[i][j] = 0;
                }else if(left.get(i - 1).equals(right.get(j - 1))){
                    matrix[i][j] = matrix[i - 1][j - 1] + 1;
                }else{
                    matrix[i][j] = Math.max(matrix[i][j - 1], matrix[i - 1][j]);
                }
            }
        }
        return matrix[lenL - 1][lenR - 1];
    }

    /**
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right) {
        Vector<String> leftV = CommonDistanceFunc.splitStringToWords(left);
        Vector<String> rightV = CommonDistanceFunc.splitStringToWords(right);
        return longestCommonSubsequence(leftV, rightV);
    }

    /**
     * Similarity = Distance / max(Left Length, Right Length)
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right) {
        Vector<String> leftV = CommonDistanceFunc.splitStringToWords(left);
        Vector<String> rightV = CommonDistanceFunc.splitStringToWords(right);
        int lenL = leftV.size();
        int lenR = rightV.size();
        int len = Math.max(lenL, lenR);
        if(len == 0) {
            return 0.0;
        }
        double distance = longestCommonSubsequence(leftV, rightV);
        return distance / len;
    }
}
