package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.Vector;

/**
 * Calculate the Levenshtein Distance.
 * Levenshtein distance: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 */

public class LevenshteinDistance implements Serializable, Similarity {

    private double levenshteinDistance(Vector<String> left, Vector<String> right) {
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
                    matrix[i][j] = Math.max(i, j);
                }else{
                    matrix[i][j] = Math.min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1);
                    matrix[i][j] = Math.min(matrix[i][j], matrix[i - 1][j - 1] + (left.get(i - 1).equals(right.get(j - 1)) ? 0 : 1));
                }
            }
        }
        return matrix[lenL - 1][lenR - 1];
    }

    /**
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right){
        Vector<String> leftV = CommonDistanceFunc.splitStringToWords(left);
        Vector<String> rightV = CommonDistanceFunc.splitStringToWords(right);
        return levenshteinDistance(leftV, rightV);
    }

    /**
     * similarity = 1.0 - Normalized Distance
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right) {
        Vector<String> leftV = CommonDistanceFunc.splitStringToWords(left);
        Vector<String> rightV = CommonDistanceFunc.splitStringToWords(right);
        return 1.0 - normalizedDistance(leftV, rightV);
    }

    /**
     * Normalized Distance = Distance / max(Left Length, Right Length)
     */
    private double normalizedDistance(Vector<String> left, Vector<String> right) {
        int lenL = left.size();
        int lenR = right.size();
        int len = Math.max(lenL, lenR);
        if(len == 0) {
            return 0.0;
        }
        double distance = levenshteinDistance(left, right);
        return distance / len;
    }
}
