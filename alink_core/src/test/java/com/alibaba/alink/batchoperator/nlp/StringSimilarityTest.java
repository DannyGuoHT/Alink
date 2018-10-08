package com.alibaba.alink.batchoperator.nlp;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.nlp.OutputTableInfo;
import com.alibaba.alink.common.nlp.StringSimilarityBase;
import com.alibaba.alink.common.nlp.StringSimilarityBatchBase;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.nlp.StringSimilarityStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StringSimilarityTest {
    private String selectedColName0 = "col0";
    private String selectedColName1 = "col1";
    private String outputColName = "output";
    private String[] keepColNames = new String[] {"ID", selectedColName0};

    @Test
    public void testStringSimilarityBatchBase() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of(new Object[] {1, "北京", "北京"})
            };
        MemSourceBatchOp words = new MemSourceBatchOp(Arrays.asList(testArray),
            new String[] {"ID", selectedColName0, selectedColName1});

        OutputTableInfo outTable = new OutputTableInfo();
        outTable.getOutputTableInfo(words.getColNames(), words.getColTypes(), selectedColName0, selectedColName1,
            outputColName, keepColNames);

        // test Output Table
        String[] outputColNames = new String[] {"output", "ID", selectedColName0};
        TypeInformation[] outTypes = new TypeInformation[] {Types.DOUBLE, Types.INT, Types.STRING};
        assertArrayEquals(outputColNames, outTable.getOutColNames());
        assertArrayEquals(outTypes, outTable.getOutTypes());

        // test stringsimilaritybatchbase
        String str = "0.0,1,北京";
        AlinkParameter params = new AlinkParameter().put(ParamName.keepColNames, keepColNames);
        StringSimilarityBatchBase base = new StringSimilarityBatchBase(params, false);
        DataSet <Row> row = words.select(outTable.getSelectedColNames()).getDataSet();
        List <Row> res = base.stringSimilarityRes(row).collect();
        assertEquals(str, res.get(0).toString());
    }

    @Test
    public void testStringSimilarityBase() throws Exception {
        AlinkParameter params = new AlinkParameter();

        Row[] array =
            new Row[] {
                Row.of(new Object[] {"北京", "北京"}),
                Row.of(new Object[] {"北京欢迎", "中国人民"}),
                Row.of(new Object[] {"Beijing", "Beijing"}),
                Row.of(new Object[] {"Beijing", "Chinese"}),
                Row.of(new Object[] {"Good Morning!", "Good Evening!"})
            };

        Double[] res = new Double[array.length];
        StringSimilarityBase base;

        // LEVENSHTEIN
        Double[] levenshtein = new Double[] {0.0, 4.0, 0.0, 6.0, 3.0};
        params.put("method", "LEVENSHTEIN");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(levenshtein, res);

        // LEVENSHTEIN_SIM
        Double[] levenshteinSim = new Double[] {1.0, 0.0, 1.0, 0.1428571428571429, 0.7692307692307692};
        params.put("method", "LEVENSHTEIN_SIM");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(levenshteinSim, res);

        // LCS
        Double[] lcs = new Double[] {2.0, 0.0, 7.0, 2.0, 10.0};
        params.put("method", "LCS");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(lcs, res);

        // LCS_SIM
        Double[] lcsSim = new Double[] {1.0, 0.0, 1.0, 0.2857142857142857, 0.7692307692307693};
        params.put("method", "LCS_SIM");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(lcsSim, res);

        // SSK
        Double[] ssk = new Double[] {1.0, 0.0, 1.0, 0.14692654669369054, 0.6636536344498839};
        params.put("method", "SSK");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(ssk, res);

        // COSINE
        Double[] cosine = new Double[] {1.0, 0.0, 1.0, 0.0, 0.5454545454545454};
        params.put("method", "COSINE").put("k", 3);
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(cosine, res);

        // SIMHASH_HAMMING
        Double[] simHash = new Double[] {0.0, 17.0, 0.0, 34.0, 21.0};
        params.put("method", "SIMHASH_HAMMING").put("k", 3);
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(simHash, res);

        // SIMHASH_HAMMING_SIM
        Double[] simHashSim = new Double[] {1.0, 0.734375, 1.0, 0.46875, 0.671875};
        params.put("method", "SIMHASH_HAMMING_SIM").put("k", 3);
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(simHashSim, res);

        // MINHASH_SIM
        Double[] minHashSim = new Double[] {1.0, 0.0, 1.0, 0.0, 0.0};
        params.put("method", "MINHASH_SIM");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(minHashSim, res);

        // JACCARD_SIM
        Double[] jaccardSim = new Double[] {1.0, 0.0, 1.0, 0.0, 0.0};
        params.put("method", "JACCARD_SIM");
        base = new StringSimilarityBase(params, false);
        for (int i = 0; i < array.length; i++) {
            res[i] = base.stringSimilairtyRes(array[i]);
        }
        assertArrayEquals(jaccardSim, res);
    }

    @Test
    public void testStringSimilarityBatch() throws Exception {
        AlinkParameter params = new AlinkParameter();
        Row[] array =
            new Row[] {
                Row.of(new Object[] {1, "北京", "北京"}),
                Row.of(new Object[] {2, "北京欢迎", "中国人民"}),
                Row.of(new Object[] {3, "Beijing", "Beijing"}),
                Row.of(new Object[] {4, "Beijing", "Chinese"}),
                Row.of(new Object[] {5, "Good Morning!", "Good Evening!"})
            };
        MemSourceBatchOp words = new MemSourceBatchOp(Arrays.asList(array),
            new String[] {"ID", selectedColName0, selectedColName1});
        StringSimilarityBatchOp evalOp =
            new StringSimilarityBatchOp()
                .setSelectedColName0(selectedColName0)
                .setSelectedColName1(selectedColName1)
                .setMethod("COSINE")
                .setK(4)
                .setOuputColName("COSINE")
                .setKeepColNames(new String[] {"ID"});
        List <Row> res = evalOp.linkFrom(words).collect();
        String[] output = {"1.0,1", "0.0,2", "1.0,3", "0.0,4", "0.4,5"};
        String[] results = new String[res.size()];
        for (int i = 0; i < res.size(); i++) {
            results[i] = res.get(i).toString();
        }
        assertArrayEquals(output, results);
    }

    @Test
    public void testStringSimilarityStream() throws Exception {
        AlinkParameter params = new AlinkParameter();
        Row[] array =
            new Row[] {
                Row.of(new Object[] {1, "北京", "北京"}),
                Row.of(new Object[] {2, "北京欢迎", "中国人民"}),
                Row.of(new Object[] {3, "Beijing", "Beijing"}),
                Row.of(new Object[] {4, "Beijing", "Chinese"}),
                Row.of(new Object[] {5, "Good Morning!", "Good Evening!"})
            };
        MemSourceStreamOp words = new MemSourceStreamOp(Arrays.asList(array),
            new String[] {"ID", selectedColName0, selectedColName1});
        StringSimilarityStreamOp evalOp =
            new StringSimilarityStreamOp()
                .setSelectedColName0(selectedColName0)
                .setSelectedColName1(selectedColName1)
                .setMethod("COSINE")
                .setK(4)
                .setOuputColName("COSINE")
                .setKeepColNames(new String[] {"ID"})
                .setTimeInterval(1);
        evalOp.linkFrom(words).print();
        StreamOperator.execute();
    }
}
