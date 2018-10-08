package com.alibaba.alink.common.nlp;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

import com.alibaba.alink.common.AlinkParameter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class KeySentencesExtraction implements Serializable {
    private Integer maxIteration, topN;
    private Double epsilon, dampingFactor;
    private TextRankConst.GraphType graphType;
    private SegmentUDF segment;
    private FilterStopUDF filter;
    private AlinkParameter params;

    private Vector <Vector <Double>> matrix;
    private Vector <Sentence> sentences;

    public KeySentencesExtraction(AlinkParameter params, SegmentUDF segment, FilterStopUDF filter) {
        this.segment = segment;
        this.filter = filter;
        this.params = params;
        topN = params.getIntegerOrDefault("topN", TextRankConst.TOPN);
        epsilon = params.getDoubleOrDefault("epsilon", TextRankConst.EPSILON);
        maxIteration = params.getIntegerOrDefault("maxIteration", TextRankConst.MAXITER);
        dampingFactor = params.getDoubleOrDefault("dampingFactor", TextRankConst.DAMPINGFACTOR);
        graphType = params.getOrDefault("graphType", TextRankConst.GraphType.class, TextRankConst.GRAPHTYPE);
    }

    public KeySentencesExtraction(AlinkParameter params) {
        this(params, null, null);
    }

    public Row getKeySentences(Row row) {
        SentenceSplitter splitter = new SentenceSplitter();
        String id = row.getField(0).toString();
        Vector <String> s = splitter.doSplit2(row.getField(1).toString());
        Vector <Sentence> src = new Vector <>();
        for (int i = 0; i < s.size(); i++) {
            src.add(new Sentence(s.get(i)));
        }
        return getKeySentences(new Tuple2 <>(id, src));
    }

    public Row getKeySentences(Tuple2 <String, Vector <Sentence>> src) {
        addContent(src.f1);
        int len = sentences.size();
        Double[] weightSum = getWeightSum();
        Double[] vec = new Double[len];
        Arrays.fill(vec, 1.0 / len);
        for (int i = 0; i < maxIteration; i++) {
            Double[] nxt = new Double[len];
            Double[] tmpVec = new Double[len];
            for (int j = 0; j < len; j++) {
                nxt[j] = vec[j] / weightSum[j];
            }
            double diff = 0.0;
            for (int j = 0; j < len; j++) {
                double z = 0;
                switch (graphType) {
                    case UNDITECTED: {
                        for (int k = 0; k < j; k++) {
                            z += matrix.get(j).get(k) * nxt[k];
                        }
                        for (int k = j + 1; k < len; k++) {
                            z += matrix.get(k).get(j) * nxt[k];
                        }
                        break;
                    }
                    case DIRECTED_FORWARD: {
                        for (int k = j + 1; k < len; k++) {
                            z += matrix.get(k).get(j) * nxt[k];
                        }
                        break;
                    }
                    case DIRECTED_BACKWARD: {
                        for (int k = 0; k < j; k++) {
                            z += matrix.get(j).get(k) * nxt[k];
                        }
                        break;
                    }
                    default: {
                        throw new RuntimeException("Not support this graph type!");
                    }
                }
                tmpVec[j] = z;
            }
            double delta = (1.0 - dampingFactor) / len;
            for (int j = 0; j < len; j++) {
                tmpVec[j] = dampingFactor * tmpVec[j] + delta;
                diff += Math.abs(vec[j] - tmpVec[j]);
            }
            vec = tmpVec.clone();
            if (diff < epsilon) {
                break;
            }
        }
        return setOutput(vec, src);
    }

    private void addContent(Vector <Sentence> doc) {
        matrix = new Vector <>();
        sentences = new Vector <>();
        StringSimilarityBase base;
        if (segment != null) {
            for (int i = 0; i < doc.size(); i++) {
                String segmentStr = segment.eval(doc.get(i).getS());
                String filterStr = filter.eval(segmentStr);
                doc.get(i).setSegmentStr(filterStr);
            }
            base = new StringSimilarityBase(params, false);
        } else {
            base = new StringSimilarityBase(params, false);

        }

        for (int i = 0; i < doc.size(); i++) {
            addSentence(doc.get(i), base);
        }
    }

    private void addSentence(Sentence src, StringSimilarityBase base) {
        Vector <Double> h = new Vector <>();
        for (int i = 0; i < sentences.size(); i++) {
            Row row = new Row(2);
            row.setField(0, sentences.get(i).getS());
            row.setField(1, src.getS());
            h.add(base.stringSimilairtyRes(row));
        }
        sentences.add(src);
        matrix.add(h);
    }

    private Row setOutput(Double[] vec, Tuple2 <String, Vector <Sentence>> t) {
        Integer[] sortIndex = sortIndex(vec);
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < sortIndex.length; i++) {
            res.append(sentences.get(sortIndex[i]).getS());
        }

        Row out = new Row(2);
        out.setField(0, t.f0);
        out.setField(1, res.toString());

        return out;
    }

    private Integer[] sortIndex(final Double[] vec) {
        int len = vec.length;
        Integer[] sortIndex = new Integer[len];
        for (int i = 0; i < len; i++) {
            sortIndex[i] = i;
        }

        Arrays.sort(sortIndex, new Comparator <Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return 0 - vec[o1].compareTo(vec[o2]);
            }
        });

        Integer[] outIndex = new Integer[Math.min(topN, len)];
        for (int i = 0; i < outIndex.length; i++) {
            outIndex[i] = sortIndex[i];
        }
        Arrays.sort(outIndex);

        return outIndex;
    }

    private Double[] getWeightSum() {
        int len = sentences.size();
        Double[] weightSum = new Double[len];
        Arrays.fill(weightSum, 0.0);
        for (int i = 0; i < len; i++) {
            switch (graphType) {
                case UNDITECTED: {
                    for (int j = 0; j < i; j++) {
                        weightSum[i] += matrix.get(i).get(j);
                    }
                    for (int j = i + 1; j < len; j++) {
                        weightSum[i] += matrix.get(j).get(i);
                    }
                    break;
                }
                case DIRECTED_FORWARD: {
                    for (int j = i + 1; j < len; j++) {
                        weightSum[i] += matrix.get(j).get(i);
                    }
                    break;
                }
                case DIRECTED_BACKWARD: {
                    for (int j = 0; j < i; j++) {
                        weightSum[i] += matrix.get(i).get(j);
                    }
                    break;
                }
                default: {
                    throw new RuntimeException("Not support this graph type!");
                }
            }
            if (weightSum[i] < 1e-18) {
                weightSum[i] = 1.0;
            }
        }
        return weightSum;
    }
}
