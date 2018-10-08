package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.Vector;

/**
 * Calculate the string subsequence kernel.
 * SSK: maps strings to a feature vector indexed by all k tuples of characters, and
 * get the dot product.
 */

public class SubsequenceKernel implements Serializable, Similarity {
    private int k;
    private double lambda;

    public SubsequenceKernel(int k, double lambda) {
        if(k < 0){
            throw new RuntimeException("k must be positive!");
        }
        this.k = k;
        this.lambda = lambda;
    }

    /**
     * Recursive computation of SSK.
     */
    private double subsequenceKernel(Vector<String> left, Vector<String> right) {
        int ll = left.size();
        int lr = right.size();
        if(ll < this.k || lr < this.k) {
            return 0.0;
        }
        double[][][] kp = new double[this.k + 1][ll][lr];
        for(int j = 0; j < ll; j++){
            for(int z = 0; z < lr; z++){
                kp[0][j][z] = 1.0;
            }
        }
        for(int i = 0; i < this.k; i++){
            for(int j = 0; j < ll - 1; j++){
                double kpp = 0.0;
                for(int z = 0; z < lr - 1; z++){
                    kpp = this.lambda * (kpp + this.lambda * (left.get(j).equals(right.get(z)) ? 1 : 0) * kp[i][j][z]);
                    kp[i + 1][j + 1][z + 1] = this.lambda * kp[i + 1][j][z + 1] + kpp;
                }
            }
        }
        double ret = 0.;
        for(int j = 0; j < ll; j++){
            for(int z = 0; z < lr; z++){
                ret += lambda * lambda * (left.get(j).equals(right.get(z)) ? 1 : 0) * kp[k - 1][j][z];
            }
        }
        return ret;
    }

    /**
     * Similarity = SSK(A,B) / (SSK(A,A) * SSK(B,B))
     * Override the similarity function of Similarity.
     */
    @Override
    public <T> double similarity(T left, T right) {
        Vector<String> leftV = CommonDistanceFunc.splitStringToWords(left);
        Vector<String> rightV = CommonDistanceFunc.splitStringToWords(right);
        double numerator = subsequenceKernel(leftV, rightV);
        double denominator = Math.sqrt(subsequenceKernel(leftV, leftV) * subsequenceKernel(rightV, rightV));
        denominator = denominator == 0. ? 1e-16 : denominator;
        double ret = numerator / denominator;

        ret = ret > 1.0 ? 1.0 : ret;
        return ret;
    }

    /**
     * Override the distance function of Similarity.
     */
    @Override
    public <T> double distance(T left, T right) {
        return 0.0;
    }
}
