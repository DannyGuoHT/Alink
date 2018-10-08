package com.alibaba.alink.common.nlp.segment;

import java.io.Serializable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

public class WordDictWithUserDefined implements Serializable {

    private WordDictionary dictionary;
    private DataSet <Row> user_defined_dict;

    public WordDictWithUserDefined() {
        dictionary = WordDictionary.getInstance();
        user_defined_dict = null;
    }

    public WordDictionary getDictionary() {
        if (dictionary == null) { dictionary = WordDictionary.getInstance(); }
        return dictionary;
    }

    public DataSet <Row> getUser_defined_dict() {
        return user_defined_dict;
    }

}
