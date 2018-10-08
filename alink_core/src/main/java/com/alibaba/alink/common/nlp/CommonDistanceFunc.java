package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * Common Functions for similarity calculation.
 */

public class CommonDistanceFunc implements Serializable {

    /**
     * Support Vector input and String input.
     * String: Split the string by character.
     * Vector: return itself.
     */
    public static <T> Vector<String> splitStringToWords(T str){
        if(str instanceof Vector){
            return (Vector)str;
        } else if(str instanceof String){
            Vector<String> res = new Vector<>();
            String s = (String) str;
            for(int i = 0; i < s.length(); i++){
                res.add(s.substring(i, i + 1));
            }
            return res;
        } else{
            throw new RuntimeException("Only support Vector and String.");
        }
    }

    /**
     * Transform the String input to Integer to support hash functions.
     * Support Vector input and String input.
     * String: return the sorted hashcode of each character.
     * Vector: return the sorted hashcode of each words.
     */
    public static <T> Vector<Integer> toInteger(T str){
        Vector<Integer> hashCode = new Vector<>();
        if(str instanceof String){
            String s = (String)str;
            for(int i = 0; i < s.length(); i++){
                hashCode.add(s.substring(i, i + 1).hashCode());
            }
        }else if(str instanceof Vector){
            Vector<String> s = (Vector)str;
            for (int i = 0; i < s.size(); i++) {
                int ret = 0;
                for (int j = 0; j < s.get(i).length(); j++) {
                    ret = 31 * ret + s.get(i).substring(j, j + 1).hashCode();
                }
                hashCode.add(ret);
            }
        } else{
            throw new RuntimeException("Only support Vector and String.");
        }
        Set<Integer> set = new HashSet<>(hashCode);
        Vector<Integer> sorted = new Vector<>(set);
        return sorted;
    }

    /**
     * Using a sliding window to split the string.
     * k: window size.
     * Support vector input and string input.
     */
    public static <T> Vector<String> splitWord2V(T str, int k){
        int len;
        if(str instanceof Vector){
            len = ((Vector)str).size();
        }else if(str instanceof String){
            len = ((String)str).length();
        }else{
            throw new RuntimeException("Only suppot Vector and String");
        }
        int range = Math.max(len - k + 1, 1);
        Vector<String> res = new Vector<>(range);
        if(str instanceof Vector){
            for(int i = 0; i < range; i++){
                StringBuilder s = new StringBuilder();
                for(int j = i; j < Math.min(i + k, len); j++){
                    s.append(((Vector)str).get(j));
                }
                res.add(s.toString());
            }
        }else{
            for(int i = 0; i < range; i++){
                res.add(((String)str).substring(i, Math.min(i + k, len)));
            }
        }
        return res;
    }
}
