package com.alibaba.alink.common.ml.feature;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.AlinkPredictor;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.constants.ParamName;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.*;


public class OneHotModelPredictor extends AlinkPredictor {

    private String[] keepColNames;
    private String[] colNames;
    private String valDelimiter;
    private String colDelimiter;
    private HashMap <String, HashMap <String, Integer>> mapping;
    private HashMap <String, Integer> res_mapping;
    private int otherMapping; // mapping for element which not in the mapping

    public OneHotModelPredictor(TableSchema modelScheme, TableSchema dataSchema, AlinkParameter params) {
        super(modelScheme, dataSchema, params);
        this.colNames = dataSchema.getColumnNames();

        if (this.keepColNames == null)
            this.keepColNames = this.colNames;
        else if(this.keepColNames.length == 0)
            this.keepColNames = this.colNames;
        else
            this.keepColNames = this.params.getStringArray(ParamName.keepColNames);

        this.valDelimiter = ":";
        this.colDelimiter = ",";
    }

    @Override
    public void loadModel(List <Row> modelRows) {
        OneHotModel model = new OneHotModel();
        model.load(modelRows);

        this.mapping = new HashMap <>();
        this.res_mapping = new HashMap <>();
        ArrayList <String> data = model.getData();
        this.otherMapping = data.size();

        for (int i = 0; i < data.size(); i++) {
            String row = data.get(i); // who:factors:name
            String[] content = row.split("@ # %", 3);
            if (content[0].equals("_reserve_col_")) {
                this.res_mapping.put(content[1], Integer.parseInt(content[2]));
            } else if (this.mapping.containsKey(content[0])) {
                this.mapping.get(content[0]).put(content[1], Integer.parseInt(content[2]));
            } else {
                HashMap <String, Integer> tmp_hash = new HashMap <>();
                tmp_hash.put(content[1], Integer.parseInt(content[2]));
                this.mapping.put(content[0], tmp_hash);
            }
        }
    }

    @Override
    public TableSchema getResultSchema() {

        String predColName = this.params.getStringOrDefault(ParamName.outputColName, null);

        int app_size = this.keepColNames.length;
        String[] outNames = new String[app_size + 1];
        TypeInformation[] outTypes = new TypeInformation[app_size + 1];

        String[] names = dataSchema.getColumnNames();
        TypeInformation[] types = dataSchema.getTypes();

        for (int i = 0; i < app_size; i++) {
            int idx = TableUtil.findIndexFromName(this.colNames, this.keepColNames[i]);
            outNames[i] = names[idx];
            outTypes[i] = types[idx];
        }
        outNames[app_size] = predColName;
        outTypes[app_size] = Types.STRING;

        return new TableSchema(outNames, outTypes);
    }

    @Override
    public Row predict(Row src) throws Exception {
        // get append column idx
        int app_size = this.keepColNames.length;
        Row row = new Row(app_size + 1);
        int iteration = 0;
        // put append col data
        for (int i = 0; i < app_size; i++) {
            int idx = TableUtil.findIndexFromName(this.colNames, this.keepColNames[i]);
            row.setField(iteration++, src.getField(idx));
        }

        String kv_str = "";
        // put reserve col value to kv string
        Iterator <Map.Entry <String, Integer>> iterator = this.res_mapping.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry <String, Integer> entry = iterator.next();
            String delimiter = kv_str.equals("") ? "" : this.colDelimiter;

            int idx = TableUtil.findIndexFromName(this.colNames, entry.getKey());
            kv_str = kv_str + delimiter + entry.getValue().toString() + this.valDelimiter + src.getField(idx).toString();
        }

        // put kv value to kv string
        Iterator <Map.Entry <String, HashMap <String, Integer>>> mapIteration = this.mapping.entrySet().iterator();
        while (mapIteration.hasNext()) {
            Map.Entry <String, HashMap <String, Integer>> entry = mapIteration.next();
            String colName = entry.getKey();
            HashMap <String, Integer> inner_map = entry.getValue();

            int idx = TableUtil.findIndexFromName(this.colNames, colName);

            String val = (src.getField(idx) != null) ? src.getField(idx).toString() : "null";
            if (inner_map.containsKey(val)) {
                int key = inner_map.get(val);

                String delimiter = kv_str.equals("") ? "" : this.colDelimiter;
                kv_str = kv_str + delimiter + key + this.valDelimiter + "1";
            } else {
                System.out.println(val);
                String delimiter = kv_str.equals("") ? "" : this.colDelimiter;
                kv_str = kv_str + delimiter + otherMapping + this.valDelimiter + "1";
            }
        }
        row.setField(iteration, kv_str);
        return row;
    }

}