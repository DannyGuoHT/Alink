package com.alibaba.alink.common.nlp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Locale;

import com.alibaba.alink.common.AlinkParameter;

public class TextRankConst {
    public final static Integer TOPN = 5;
    public final static Integer WINDOWSIZE = 2;
    public final static Double DAMPINGFACTOR = 0.85;
    public final static Integer MAXITER = 100;
    public final static Double EPSILON = 0.000001;
    public final static Integer TIMEINTERVAL = 3;
    public final static String DELIMITER = "。？！";
    public final static GraphType GRAPHTYPE = GraphType.UNDITECTED;
    public final static String STOPWORDDICT = "/stop.txt";

    public enum GraphType {
        /**
         * <code>UNDITECTED</code>
         */
        UNDITECTED,
        /**
         * <code>DIRECTED_FORWARD</code>;</code>
         */
        DIRECTED_FORWARD,
        /**
         * <code>DIRECTED_BACKWARD</code>
         */
        DIRECTED_BACKWARD
    }

    public static KeySentencesExtraction getSentenceClass(AlinkParameter params, InputStream is) {
        final Boolean segment = params.getBoolOrDefault("segment", false);

        KeySentencesExtraction keySentence;
        if (segment) {
            SegmentUDF segmentUdf = new SegmentUDF(" ");
            HashSet <String> stopWordsSet = new HashSet <>();
            loadDefaultStopDict(stopWordsSet, is);
            FilterStopUDF filter = new FilterStopUDF(stopWordsSet, " ");
            keySentence = new KeySentencesExtraction(params, segmentUdf, filter);
        } else {
            keySentence = new KeySentencesExtraction(params);
        }
        return keySentence;
    }

    static void loadDefaultStopDict(HashSet <String> stopWordsSet, InputStream is) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            while (br.ready()) {
                String line = br.readLine();
                if (line.length() > 0) {
                    stopWordsSet.add(line);
                }
            }

        } catch (IOException e) {
            System.err.println(String.format(Locale.getDefault(), "%s close failure!", STOPWORDDICT));
        }

    }
}
