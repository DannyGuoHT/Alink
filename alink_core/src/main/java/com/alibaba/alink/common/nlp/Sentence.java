package com.alibaba.alink.common.nlp;

import java.io.Serializable;

public class Sentence implements Serializable {
    private String src;
    private String segmentStr;
    private Double weight;

    public Sentence(String src){
        this(src,0.0);
    }

    public Sentence(String src, Double weight){
        this(src, null, 0.0);
    }

    public Sentence(String src, String segmentStr, Double weight){
        this.src = src;
        this.segmentStr = segmentStr;
        this.weight = weight;
    }

    public String getSegmentStr() {
        return segmentStr;
    }

    public void setSegmentStr(String segmentStr) {
        this.segmentStr = segmentStr;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public String getS() {
        return src;
    }
}
