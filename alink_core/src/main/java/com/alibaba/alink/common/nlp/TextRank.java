package com.alibaba.alink.common.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.*;

/**
 * Textrank: exploits the structure of the text itself to determine keyphrases that appear "central"
 * to the text in the same way that PageRank selects important Web pages.
 */
public class TextRank implements Serializable {
    Integer topN;
    Integer windowSize;
    Double dampingFactor;
    Double epsilon;
    Integer maxIteration;

    /**
     * topN: Output the top N importance words.
     * windowSize: the size of co-occur window for creating undirected graph.
     * dampingFactor: damping factor for text rank.
     * epsilon: threshold where computation iteration stops.
     * maxIteration: max iteration to convergence.
     */
    public TextRank(AlinkParameter params){
        topN = params.getIntegerOrDefault("topN", TextRankConst.TOPN);
        windowSize = params.getIntegerOrDefault("windowSize", TextRankConst.WINDOWSIZE);
        dampingFactor = params.getDoubleOrDefault("dampingFactor", TextRankConst.DAMPINGFACTOR);
        epsilon = params.getDoubleOrDefault("epsilon", TextRankConst.EPSILON);
        maxIteration = params.getIntegerOrDefault("maxIteration", TextRankConst.MAXITER);
    }

    /**
     * Apply graph-based ranking algorithm.
     */
    public Row[] getKeyWords(Row row){
        Vector<String> id2Word = new Vector<>();
        Map<String, Integer> word2Id = new HashMap<>();
        // Create the undirected graph.
        double[][] matrix = getWordId(row, id2Word, word2Id);
        int len = id2Word.size();
        double[] countSide = new double[len];
        for(int i = 0; i < len; i++){
            for(int j = 0; j < len; j++){
                countSide[i] += matrix[i][j];
            }
        }
        for(int i = 0; i < len; i++){
            for(int j = 0; j < len; j++){
                matrix[i][j] /= countSide[j];
            }
        }

        Double[] n1 = new Double[len];
        Double[] vec = new Double[len];
        Arrays.fill(n1, (1.0 - dampingFactor) / len);
        Arrays.fill(vec, 1.0 / len);
        // Iterate the graph-based ranking algorithm until convergence.
        for(int i = 0; i < maxIteration; i++){
            Double[] nxt = new Double[len];
            double max = -1.0;
            for(int j = 0; j < len; j++){
                double tmp = 0.0;
                for(int k = 0; k < len; k++){
                    tmp += matrix[j][k] * vec[k];
                }
                nxt[j] = n1[j] + dampingFactor * tmp;
                max = Math.max(max, Math.abs(nxt[j] - vec[j]));
            }
            vec = nxt.clone();
            if(max < epsilon){
                break;
            }
        }

        return setOutput(vec, row, id2Word);
    }

    /**
     * @param vec: vector to be sorted.
     * @return the original index after sorted.
     */
    private Integer[] sortIndex(final Double[] vec){
        int len = vec.length;
        Integer[] sortIndex = new Integer[len];
        for(int i = 0; i < len; i++){
            sortIndex[i] = i;
        }

        Arrays.sort(sortIndex, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return 0 - vec[o1].compareTo(vec[o2]);
            }
        });

        return sortIndex;
    }

    /**
     * Create the map between word and ID, and record the undirected graph.
     * @param row: article
     * @param id2Word: words list with ID as index.
     * @param word2Id: map between word and ID, word is the index.
     * @return two-dimension matrix, which records the co-occur relationship within a window between words.
     */
    private double[][] getWordId(Row row, Vector<String> id2Word, Map<String, Integer> word2Id){
        Vector<String> words = new Vector<>(Arrays.asList(row.getField(1).toString().split(" +")));
        Integer ct = 0;
        for(int i = 0; i < words.size(); i++){
            if(!word2Id.containsKey(words.get(i))){
                word2Id.put(words.get(i), ct++);
                id2Word.add(words.get(i));
            }
        }
        int len = id2Word.size();
        double[][] matrix = new double[len][len];
        for(int i = 0; i < words.size(); i++){
            int end = i + windowSize;
            end = end < words.size() ? end : words.size();
            for(int j = i + 1; j < end; j++){
                if(!words.get(i).equals(words.get(j))){
                    matrix[word2Id.get(words.get(i))][word2Id.get(words.get(j))] = 1.0;
                    matrix[word2Id.get(words.get(j))][word2Id.get(words.get(i))] = 1.0;
                }
            }
        }
        return matrix;
    }

    /**
     * Sort the scored vector.
     * @param vec: scored words ID.
     * @param row: Input row.
     * @param id2Word: words list with ID as index.
     * @return the top N importance words.
     */
    private Row[] setOutput(Double[] vec, Row row, Vector<String> id2Word){
        int len = vec.length;
        Integer top = Math.min(topN, len);
        Row[] out = new Row[top];
        Integer[] sortIndex = sortIndex(vec);

        for(int i = 0; i < top; i++){
            out[i] = new Row(3);
            out[i].setField(0, row.getField(0));
            out[i].setField(1, id2Word.get(sortIndex[i]));
            out[i].setField(2, vec[sortIndex[i]]);
        }
        return out;
    }
}
