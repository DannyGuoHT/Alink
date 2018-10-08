package com.alibaba.alink.batchoperator.nlp;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.nlp.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Calculate the similarity between characters in pair.
 * We support different similarity methods, by choosing the parameter "method", you can choose
 * which method you use.
 * LEVENSHTEIN: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 * LEVENSHTEIN_SIM: similarity = 1.0 - Normalized Distance.
 * LCS: the longest subsequence common to the two inputs.
 * LCS_SIM: Similarity = Distance / max(Left Length, Right Length)
 * COSINE: a measure of similarity between two non-zero vectors of an inner product
 * space that measures the cosine of the angle between them.
 * SSK: maps strings to a feature vector indexed by all k tuples of characters, and
 * get the dot product.
 * SIMHASH_HAMMING: Hash the inputs to BIT_LENGTH size, and calculate the hamming distance.
 * SIMHASH_HAMMING_SIM: Similarity = 1.0 - distance / BIT_LENGTH.
 * MINHASH_SIM: MinHashSim = P(hmin(A) = hmin(B)) = Count(I(hmin(A) = hmin(B))) / k.
 * JACCARD_SIM: JaccardSim = |A ∩ B| / |A ∪ B| = |A ∩ B| / (|A| + |B| - |A ∩ B|)
 */

public class StringSimilarityBatchOp extends BatchOperator implements Serializable {
    /**
     * default constructor.
     */
    public StringSimilarityBatchOp(){
        super(null);
    }

    public StringSimilarityBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * constructor.
     * @param selectedColName0 the first string col
     * @param selectedColName1 the second string col
     * @param method similarity method
     * @param k window size for SSK, COSINE and SIMHASH
     * @param lambda parameter for SSK
     * @param seed random seed for MINHASH
     * @param minHashK parameter for MINHASH
     * @param bucket parameter for MINHASH
     */
    public StringSimilarityBatchOp(String selectedColName0, String selectedColName1, String method, Integer k,
                                   Double lambda, Integer seed, Integer minHashK, Integer bucket) {
        this(new AlinkParameter()
                .put("selectedColName0", selectedColName0)
                .put("selectedColName1", selectedColName1)
                .put("method", method)
                .put("k", k)
                .put("lambda", lambda)
                .put("seed", seed)
                .put("minHashK", minHashK)
                .put("bucket", bucket));
    }

    /**
     * Set the first selected string col.
     * @param value selected colname
     * @return this
     */
    public StringSimilarityBatchOp setSelectedColName0(String value){
        putParamValue("selectedColName0", value);
        return this;
    }

    /**
     * Set the second selected string col.
     * @param value selected colname
     * @return this
     */
    public StringSimilarityBatchOp setSelectedColName1(String value){
        putParamValue("selectedColName1", value);
        return this;
    }

    /**
     * Set the similarity method.
     * @param value method
     * @return this
     */
    public StringSimilarityBatchOp setMethod(String value){
        putParamValue("method", value);
        return this;
    }

    /**
     * Set the window size for SSK, COSINE and SIMHASH
     * @param value window size
     * @return this
     */
    public StringSimilarityBatchOp setK(Integer value){
        putParamValue("k", value);
        return this;
    }

    /**
     * Set the parameter for SSK
     * @param value lambda
     * @return this
     */
    public StringSimilarityBatchOp setLambda(Double value){
        putParamValue("lambda", value);
        return this;
    }

    /**
     * Set the random seed for MINHASH and JACCARD
     * @param value random seed
     * @return this
     */
    public StringSimilarityBatchOp setSeed(Integer value){
        putParamValue("seed", value);
        return this;
    }

    /**
     * Set the number of hash functions used in MINHASH and JACCARD
     * @param value the number of hash functions
     * @return this
     */
    public StringSimilarityBatchOp setMinHashK(Integer value){
        putParamValue("minHashK", value);
        return this;
    }

    /**
     * Set the number of bucket for LSH
     * @param value the number of bucket
     * @return this
     */
    public StringSimilarityBatchOp setBucket(Integer value){
        putParamValue("bucket", value);
        return this;
    }

    /**
     * Set the output colname
     * @param value output colname
     * @return this
     */
    public StringSimilarityBatchOp setOuputColName(String value){
        putParamValue(ParamName.outputColName, value);
        return this;
    }

    /**
     * Set the keep colnames
     * @param value keep colnames
     * @return this
     */
    public StringSimilarityBatchOp setKeepColNames(String[] value){
        putParamValue(ParamName.keepColNames, value);
        return this;
    }

    /**
     * the link from data
     * @param in input operator, which contains the data.
     * @return
     */
    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        String selectedColName0 = this.params.getString("selectedColName0");
        String selectedColName1 = this.params.getString("selectedColName1");
        String outputColName = this.params.getStringOrDefault(ParamName.outputColName, StringSimilarityConst.OUTPUT);
        String[] keepColNames = this.params.getStringArrayOrDefault(ParamName.keepColNames, StringSimilarityConst.KEEPCOLNAMES);

        // Set the output colNames and types
        OutputTableInfo outTable = new OutputTableInfo();
        outTable.getOutputTableInfo(in.getColNames(), in.getColTypes(), selectedColName0, selectedColName1, outputColName, keepColNames);

        // Get the input data.
        DataSet<Row> data = in.select(outTable.getSelectedColNames()).getDataSet();

        // Calculate the similarity in pair.
        StringSimilarityBatchBase base = new StringSimilarityBatchBase(this.params, false);
        DataSet<Row> out = base.stringSimilarityRes(data);

        // Set the output into table.
        this.table = RowTypeDataSet.toTable(out, outTable.getOutColNames(), outTable.getOutTypes());

        return this;
    }

}
