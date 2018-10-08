package com.alibaba.alink.batchoperator.utils;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class JsonValueBatchOp extends BatchOperator {

    private String selectedColName;
    private String outputColName;
    private String[] keepColNames;
    private String jsonPath;

    public JsonValueBatchOp(String selectedColName, String outputColName, String jsonPath) {
        this(selectedColName, outputColName, null, jsonPath);
    }

    public JsonValueBatchOp(String selectedColName, String outputColName, String[] keepColNames, String jsonPath) {
        this(new AlinkParameter()
            .put(ParamName.selectedColName, selectedColName)
            .put(ParamName.outputColName, outputColName)
            .put(ParamName.keepColNames, keepColNames)
            .put("JsonPath", jsonPath)
        );
    }

    public JsonValueBatchOp(AlinkParameter param) {
        super(param);
        this.selectedColName = param.getString(ParamName.selectedColName);
        this.keepColNames = param.getStringArrayOrDefault(ParamName.keepColNames, null);
        this.outputColName = param.getString(ParamName.outputColName);
        this.jsonPath = param.getString("JsonPath");
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {
        if (this.keepColNames == null) {
            this.keepColNames = in.getColNames();
        }

        int[] keepIdx = new int[keepColNames.length];

        String[] outNames = new String[keepColNames.length + 1];
        TypeInformation[] types = new TypeInformation[keepColNames.length + 1];

        for (int i = 0; i < keepColNames.length; ++i) {
            outNames[i] = keepColNames[i];
            keepIdx[i] = TableUtil.findIndexFromName(in.getColNames(), keepColNames[i]);
            types[i] = in.getColTypes()[keepIdx[i]];
        }
        int selIdx = TableUtil.findIndexFromName(in.getColNames(), selectedColName);
        outNames[keepIdx.length] = outputColName;
        types[keepIdx.length] = Types.STRING();

        DataSet<Row> out = in.getDataSet().mapPartition(new Transform(this.jsonPath, selIdx, keepIdx));

        this.table = RowTypeDataSet.toTable(out, new TableSchema(outNames, types));

        return this;
    }

    public static class Transform implements MapPartitionFunction<Row, Row> {

        private int selIdx;
        private int[] keepIdx;
        private int colSize;
        private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        private String jsonPath;

        public Transform(String jsonPath, int selIdx, int[] keepIdx) {
            this.jsonPath = jsonPath;
            this.selIdx = selIdx;
            this.keepIdx = keepIdx;
            colSize = keepIdx.length + 1;
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<Row> collector) throws Exception {
            JsonPath path = JsonPath.compile(jsonPath);
            for (Row row : iterable) {
                Row ret = new Row(this.colSize);
                for (int i = 0; i < this.keepIdx.length; ++i) {
                    ret.setField(i, row.getField(this.keepIdx[i]));
                }

                String json = (String)row.getField(selIdx);
                if (null == json) {
                    collector.collect(ret);
                    continue;
                }
                Object obj = "";
                try {
                    obj = path.read(json);
                } catch (Exception e) {
                    if (e instanceof NullPointerException) {
                        obj = null;
                    }
                }
                if (obj instanceof String) {
                    ret.setField(colSize - 1, obj);
                } else {
                    ret.setField(colSize - 1, gson.toJson(obj));
                }
                collector.collect(ret);
            }
        }
    }

}
