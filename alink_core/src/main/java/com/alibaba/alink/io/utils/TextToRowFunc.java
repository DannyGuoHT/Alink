package com.alibaba.alink.io.utils;

import com.alibaba.alink.common.AlinkParameter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.alibaba.alink.common.AlinkSession.gson;


public class TextToRowFunc extends RichFlatMapFunction<String, Row> {
    private AlinkParameter params;
    private String schemaStr;
    private DataFormat dataFormat;
    private LengthCheck lengthCheck;

    private String fieldDelimiter;
    private String colDelimiter;
    private String valDelimiter;

    public enum DataFormat implements Serializable {
        plain, csv, json, kv
    }

    public enum LengthCheck implements Serializable {
        skip, pad, exception
    }

    public TextToRowFunc(AlinkParameter params) {
        this.params = params;

        String lengthCheckStr = params.getStringOrDefault("lengthCheck", "skip");
        String dataFormatStr = params.getString("dataFormat");
        this.schemaStr = params.getString("schemaStr");

        if (lengthCheckStr.equalsIgnoreCase("skip"))
            this.lengthCheck = LengthCheck.skip;
        else if (lengthCheckStr.equalsIgnoreCase("pad"))
            this.lengthCheck = LengthCheck.pad;
        else if (lengthCheckStr.equalsIgnoreCase("exception"))
            this.lengthCheck = LengthCheck.exception;
        else
            throw new IllegalArgumentException("Not supported length check type: " + lengthCheckStr);

        if (dataFormatStr.equalsIgnoreCase("plain"))
            this.dataFormat = DataFormat.plain;
        else if (dataFormatStr.equalsIgnoreCase("csv"))
            this.dataFormat = DataFormat.csv;
        else if (dataFormatStr.equalsIgnoreCase("json"))
            this.dataFormat = DataFormat.json;
        else if (dataFormatStr.equalsIgnoreCase("kv"))
            this.dataFormat = DataFormat.kv;
        else
            throw new IllegalArgumentException("Not supported data format: " + dataFormatStr);

        if (dataFormat == DataFormat.csv) {
            this.fieldDelimiter = params.getStringOrDefault("fieldDelimiter", ",");
            this.fieldDelimiter = Pattern.quote(CsvUtil.unEscape(this.fieldDelimiter));
        } else if (dataFormat == DataFormat.kv) {
            this.colDelimiter = params.getStringOrDefault("colDelimiter", ",");
            this.colDelimiter = Pattern.quote(CsvUtil.unEscape(this.colDelimiter));
            this.valDelimiter = params.getStringOrDefault("valDelimiter", ":");
            this.valDelimiter = Pattern.quote(CsvUtil.unEscape(this.valDelimiter));
        }
    }

    private transient String[] colNames;
    private transient TypeInformation[] colTypes;
    private transient Class[] colClasses;
    private transient Row reused;
    private transient Map<String, Integer> keyToFieldIdx;
    private transient int flag[];

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        colNames = CsvUtil.getColNames(schemaStr);
        colTypes = CsvUtil.getColTypes(schemaStr);
        reused = new Row(colNames.length);

        colClasses = new Class[colTypes.length];
        for (int i = 0; i < colTypes.length; i++) {
            colClasses[i] = CsvUtil.typeToClass(colTypes[i]);
        }

        keyToFieldIdx = new HashMap<>();
        for (int i = 0; i < colNames.length; i++) {
            keyToFieldIdx.put(colNames[i], i);
        }

        flag = new int[colNames.length];
    }

    @Override
    public void flatMap(String value, Collector<Row> out) throws Exception {
        boolean succ;

        if (dataFormat == DataFormat.plain) {
            reused.setField(0, value);
            succ = true;
        } else if (dataFormat == DataFormat.csv) {
            succ = parseCsv(value);
        } else if (dataFormat == DataFormat.json) {
            succ = parseJson(value);
        } else if (dataFormat == DataFormat.kv) {
            succ = parseKv(value);
        } else {
            throw new RuntimeException("not supported format: " + this.dataFormat);
        }

        if (succ) {
            out.collect(reused);
        } else {
            if (lengthCheck == LengthCheck.exception)
                throw new RuntimeException("Fail to parse data: " + value);
            else if (lengthCheck == LengthCheck.pad)
                out.collect(reused);
        }
    }

    private boolean parseCsv(String data) {
        for (int i = 0; i < reused.getArity(); i++) {
            reused.setField(i, null);
        }

        if (data == null)
            return false;

        String[] fields = data.split(fieldDelimiter);
        boolean succ = true;

        for (int i = 0; i < reused.getArity(); i++) {
            if (i < fields.length) {
                if (colClasses[i].equals(String.class)) {
                    reused.setField(i, fields[i]);
                } else {
                    Object o;
                    try {
                        o = gson.fromJson(fields[i], colClasses[i]);
                    } catch (Exception e) {
                        o = null;
                        succ = false;
                    }
                    reused.setField(i, o);
                }
            } else {
                succ = false;
                reused.setField(i, null);
            }
        }
        return succ;
    }

    private boolean parseKv(String data) {
        String[] fields = data.split(colDelimiter);
        Arrays.fill(flag, 0);

        for (int i = 0; i < reused.getArity(); i++) {
            reused.setField(i, null);
        }

        for (int i = 0; i < fields.length; i++) {
            if (StringUtils.isNullOrWhitespaceOnly(fields[i]))
                continue;
            String[] kv = fields[i].split(valDelimiter);
            if (kv.length < 2)
                continue;
            Integer fidx = keyToFieldIdx.get(kv[0]);
            if (fidx == null)
                continue;

            try {
                reused.setField(fidx, gson.fromJson(kv[1], colClasses[fidx]));
                flag[fidx] = 1;
            } catch (Exception e) {
            }
        }

        for (int i = 0; i < flag.length; i++) {
            if (flag[i] == 0)
                return false;
        }
        return true;
    }

    private boolean parseJson(String data) {
        boolean succ = true;

        for (int i = 0; i < reused.getArity(); i++) {
            reused.setField(i, null);
        }

        HashMap<String, Object> maps = null;

        try {
            maps = gson.fromJson(data, HashMap.class);
        } catch (Exception e) {
            return false;
        }

        try {
            for (int i = 0; i < colNames.length; i++) {
                if (maps.containsKey(colNames[i])) {
                    Object value = maps.get(colNames[i]);
                    if (colTypes[i].equals(Types.STRING)) {
                        reused.setField(i, String.valueOf(value));
                    } else {
                        if (value.getClass().equals(String.class)) {
                            reused.setField(i, gson.fromJson((String) value, colClasses[i]));
                        } else {
                            if (colTypes[i].equals(Types.LONG)) {
                                reused.setField(i, (long) ((double) value));
                            } else if (colTypes[i].equals(Types.INT)) {
                                reused.setField(i, (int) ((double) value));
                            } else {
                                reused.setField(i, value);
                            }
                        }
                    }
                } else {
                    reused.setField(i, null);
                    succ = false;
                }
            }
        } catch (Exception e) {
            succ = false;
        }

        return succ;
    }
}
