package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class SentenceSplitter implements Serializable {
    Set<Character> delimiter;

    public SentenceSplitter(){
        this(TextRankConst.DELIMITER);
    }

    public SentenceSplitter(final String delimiter){
        this.delimiter = new HashSet<>();
        for(int i = 0; i < delimiter.length(); i++){
            this.delimiter.add(delimiter.charAt(i));
        }
    }

    public Vector<String> doSplit(final String src){
        Vector<String> ret = new Vector<>();
        StringBuilder cur = new StringBuilder();
        int c = 0, len = src.length();
        char v;
        while(true){
            if(c == len){
                ret.add(cur.toString());
                break;
            }
            v = src.charAt(c++);
            long cnt = 0;
            while(delimiter.contains(v)){
                cur.append(v);
                cnt++;
                if(c != len){
                    v = src.charAt(c++);
                }else{
                    ret.add(cur.toString());
                    return ret;
                }
            }
            if(cnt > 0){
                ret.add(cur.toString());
                cur = new StringBuilder();
            }
            cur.append(v);
        }
        return ret;
    }

    public Vector<String> doSplit2(final String src){
        Vector<String> ret = new Vector<>();
        int pre = 0, d = Integer.MIN_VALUE;
        for(int i = 0; i < src.length(); i++){
            if(delimiter.contains(src.charAt(i))){
                d = i;
            }else{
                if(d == i - 1){
                    ret.add(src.substring(pre, d + 1));
                    pre = d + 1;
                }
            }
        }
        if(pre != src.length()){
            ret.add(src.substring(pre, src.length()));
        }
        return ret;
    }
}
