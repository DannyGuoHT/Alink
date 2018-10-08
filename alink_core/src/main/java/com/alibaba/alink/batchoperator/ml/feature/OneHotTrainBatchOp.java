package com.alibaba.alink.batchoperator.ml.feature;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.ml.MLModel;
import com.alibaba.alink.common.ml.feature.OneHotModel;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * *
 * A one-hot batch operator that maps a serial of columns of category indices to a column of
 * sparse binary vectors. the train OP will produce a model of one hot, and using this model
 * the encoding OP can transform the data to binary format.
 *
 */
public class OneHotTrainBatchOp extends BatchOperator {
    final private static String NULL_VALUE = "null";

    /**
     * null constructor.
     */
    public OneHotTrainBatchOp() {
        super(null);
    }

    /**
     * constructor.
     *
     * @param params the parameters set.
     */
    public OneHotTrainBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * constructor.
     *
     * @param selectedColNames the column names which will be selected to train the model.
     * @param reserveColNames  the column names that will be reserved to the kv string.
     * @param ignoreNull       ignore the null value.
     * @param dropLast         drop last element of not.
     */
    public OneHotTrainBatchOp(String[] selectedColNames, String[] reserveColNames,
                              boolean ignoreNull, boolean dropLast) {
        super(new AlinkParameter()
            .put(ParamName.selectedColNames, selectedColNames)
            .put("reserveColNames", reserveColNames)
            .put("ignoreNull", ignoreNull)
            .put("dropLast", dropLast));
    }

    /**
     * set selected column names
     *
     * @param value selected column names
     * @return this
     */
    public OneHotTrainBatchOp setSelectedColNames(String[] value) {
        putParamValue(ParamName.selectedColNames, value);
        return this;
    }

    /**
     * set reserve column names
     *
     * @param value reserve column names
     * @return this
     */
    public OneHotTrainBatchOp setReserveColNames(String[] value) {
        putParamValue("reserveColNames", value);
        return this;
    }

    /**
     * set dropLast
     *
     * @param value true or false, if true drop the last element of enum.
     * @return this
     */
    public OneHotTrainBatchOp setDropLast(boolean value) {
        putParamValue("dropLast", value);
        return this;
    }

    /**
     * set ignoreNull
     *
     * @param value true or false, if true ignore the null values.
     * @return this
     */
    public OneHotTrainBatchOp setIgnoreNull(boolean value) {
        putParamValue("ignoreNull", value);
        return this;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        /**
         * coding columns names
         **/
        String[] selectedColNames = params.getStringArray(ParamName.selectedColNames);
        /**
         * columns reserve to the kv string which store coding results
         **/
        String[] reserveColNames = params.getStringArrayOrDefault("reserveColNames", new String[] {});
        /**
         * the type to processing the NULL values
         **/
        boolean ignoreNull = params.getBoolOrDefault("ignoreNull", Boolean.FALSE);
        /**
         * drop the last coding value for nonlinear of kv vectors
         **/
        boolean dropLast = params.getBoolOrDefault("dropLast", Boolean.FALSE);

        String modelName = "OneHot";

        String[] colNames = in.getColNames();
        int[] idx = new int[selectedColNames.length];
        for (int i = 0; i < selectedColNames.length; ++i) {
            idx[i] = TableUtil.findIndexFromName(colNames, selectedColNames[i]);
        }

        // create the model meta
        AlinkParameter meta = new AlinkParameter()
            .put("modelName", modelName);

        /**
         * train processing: generate the model mapping.
         * we use HashSet to store the different enum value.
         */
        DataSet<Row> mapping = in.getDataSet()
            .mapPartition(new ParseItem(selectedColNames, idx))
            .mapPartition(new ReduceItem(reserveColNames, dropLast, ignoreNull, meta))
            .setParallelism(1);

        this.table = RowTypeDataSet.toTable(mapping, MLModel.getModelSchemaWithType(Types.STRING));
        return this;
    }

    public static class ReduceItem implements MapPartitionFunction<Row, Row> {
        private String[] reserveColNames;
        private boolean dropLast;
        private boolean ignoreNull;
        private AlinkParameter meta = null;

        public ReduceItem(String[] reserveColNames, boolean dropLast, boolean ignoreNull, AlinkParameter meta) {
            this.reserveColNames = reserveColNames;
            this.dropLast = dropLast;
            this.ignoreNull = ignoreNull;
            this.meta = meta;
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<Row> collector) throws Exception {
            Map<String, HashSet<String>> map = new HashMap<>(0);
            for (Row row : rows) {
                String colName = row.getField(0).toString();
                String value = row.getField(1).toString();
                if (map.containsKey(colName)) {
                    map.get(colName).add(value);
                } else {
                    HashSet<String> set = new HashSet<>();
                    set.add(value);
                    map.put(colName, set);
                }
            }

            // data for model
            ArrayList<String> data = new ArrayList<String>();

            int mapIteration = 0;

            /**
             * the delimiter of element in the model is "@ # $"
             */
            int resSize = this.reserveColNames.length;
            for (int i = 0; i < resSize; ++i) {
                String tmp = "_reserve_col_" + "@ # %"
                    + this.reserveColNames[i] + "@ # %"
                    + mapIteration++;
                data.add(tmp);
            }

            Iterator<Map.Entry<String, HashSet<String>>> iterator = map.entrySet().iterator();
            Map.Entry<String, HashSet<String>> firstEntry = iterator.next();
            while (iterator.hasNext()) {
                Map.Entry<String, HashSet<String>> entry = iterator.next();
                String name = entry.getKey();
                HashSet<String> values = entry.getValue();
                for (Iterator<String> it = values.iterator(); it.hasNext(); ) {

                    String val = it.next();
                    if (ignoreNull && val.equalsIgnoreCase(NULL_VALUE)) {
                        continue;
                    }
                    String tmp = name + "@ # %"
                        + val + "@ # %"
                        + mapIteration++;
                    data.add(tmp);
                }
            }

            String name = firstEntry.getKey();
            HashSet<String> values = firstEntry.getValue();
            Iterator<String> it = values.iterator();
            String firstValue = it.next();
            while (it.hasNext()) {
                String val = it.next();
                if (ignoreNull && val.equalsIgnoreCase(NULL_VALUE)) {
                    continue;
                }
                String tmp = name + "@ # %"
                    + val + "@ # %"
                    + mapIteration++;
                data.add(tmp);
            }

            if (!dropLast) {
                if (!(ignoreNull && firstValue.equalsIgnoreCase(NULL_VALUE))) {
                    String tmp = name + "@ # %"
                        + firstValue + "@ # %"
                        + mapIteration++;
                    data.add(tmp);
                }
            }

            OneHotModel model = new OneHotModel(this.meta, data);
            // meta plus data
            List<Row> modelRows = model.save();
            for (Row row : modelRows) {
                collector.collect(row);
            }
        }
    }

    public static class ParseItem implements MapPartitionFunction<Row, Row> {
        private String[] binaryColNames;
        private int[] idx;

        public ParseItem(String[] binaryColNames, int[] idx) {
            this.binaryColNames = binaryColNames;
            this.idx = idx;
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<Row> collector) throws Exception {
            Map<String, HashSet<String>> map = new HashMap<>(0);
            int m = this.binaryColNames.length;
            for (Row row : rows) {
                for (int i = 0; i < m; i++) {
                    String colName = this.binaryColNames[i];
                    Object obj = row.getField(idx[i]);
                    String value = (obj == null) ? "null" : obj.toString();
                    if (map.containsKey(colName)) {
                        HashSet<String> set = map.get(colName);
                        set.add(value);
                    } else {
                        HashSet<String> set = new HashSet<>();
                        set.add(value);
                        map.put(colName, set);
                    }
                }
            }
            for (Map.Entry<String, HashSet<String>> entry : map.entrySet()) {

                String name = entry.getKey();
                HashSet<String> values = entry.getValue();
                for (Iterator it = values.iterator(); it.hasNext(); ) {
                    Row r = new Row(2);
                    r.setField(0, name);
                    r.setField(1, it.next());
                    collector.collect(r);
                }
            }
        }
    }
}