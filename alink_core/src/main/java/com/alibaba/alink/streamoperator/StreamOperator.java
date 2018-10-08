package com.alibaba.alink.streamoperator;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.streamoperator.dataproc.SampleStreamOp;
import com.alibaba.alink.streamoperator.source.TableSourceStreamOp;
import com.alibaba.alink.streamoperator.sql.*;
import com.alibaba.alink.streamoperator.utils.udf.UDFStreamOp;
import com.alibaba.alink.streamoperator.utils.PrintStreamOp;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.plan.schema.RelTable;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public abstract class StreamOperator {

    protected AlinkParameter params;

    protected Table table = null;

    private String tableName = null;

    protected Table[] sideTables = null;

    protected StreamOperator(AlinkParameter params) {
        if (null == params) {
            this.params = new AlinkParameter();
        } else {
            this.params = params.clone();
        }
    }

    public AlinkParameter getParams() {
        return this.params.clone();
    }

    public DataStream<Row> getDataStream() {
        return RowTypeDataStream.fromTable(this.table);
    }

    protected void setTable(DataStream<Row> dataSet, TableSchema schema) {
        this.table = RowTypeDataStream.toTable(dataSet, schema);
    }

    protected void setTable(DataStream<Row> dataSet) {
        this.table = RowTypeDataStream.toTable(dataSet);
    }

    protected void setTable(DataStream<Row> dataSet, String[] colNames) {
        this.table = RowTypeDataStream.toTable(dataSet, colNames);
    }

    protected void setTable(DataStream<Row> dataSet, String[] colNames, TypeInformation<?>[] colTypes) {
        this.table = RowTypeDataStream.toTable(dataSet, colNames, colTypes);
    }

    public Table getTable() {
        return this.table;
    }

    public String getTableName() {
        if (null == tableName) {
            tableName = this.table.toString();
        }
        return tableName;
    }

    public StreamOperator setTableName(String name) {
        AlinkSession.getStreamTableEnvironment().registerTable(name, this.table);
        this.tableName = name;
        return this;
    }

    public StreamOperator forceSetTableName(String name) {
        if (AlinkSession.getStreamTableEnvironment().isRegistered(name)) {
            AlinkSession.getStreamTableEnvironment().replaceRegisteredTable(name,
                new RelTable(this.table.getRelNode()));
        } else {
            AlinkSession.getStreamTableEnvironment().registerTable(name, this.table);
        }
        this.tableName = name;
        return this;
    }

    public static String createUniqueTableName() {
        return TableUtil.getTempTableName();
    }

    public String[] getColNames() {
        return getSchema().getColumnNames();
    }

    public TypeInformation<?>[] getColTypes() {
        return getSchema().getTypes();
    }

    public TableSchema getSchema() {
        return this.table.getSchema();
    }

    @Override
    public String toString() {
        return this.table.toString();
    }

    public StreamOperator sample(double ratio) {
        return linkTo(new SampleStreamOp(ratio));
    }

    public StreamOperator sample(double ratio, long maxSamples) {
        return linkTo(new SampleStreamOp(ratio, maxSamples));
    }

    public StreamOperator getSideOutput() {
        return getSideOutput(0);
    }

    public StreamOperator getSideOutput(int idx) {
        if (null == this.sideTables) {
            throw new RuntimeException("There is no side output.");
        } else if (idx < 0 && idx >= this.sideTables.length) {
            throw new RuntimeException("There is no  side output.");
        } else {
            return new TableSourceStreamOp(this.sideTables[idx]);
        }
    }

    public int getSideOutputCount() {
        return null == this.sideTables ? 0 : this.sideTables.length;
    }

    public StreamOperator print() {
        return linkTo(new PrintStreamOp());
    }

    public StreamOperator link(StreamOperator f) {
        return linkTo(f);
    }

    public StreamOperator linkTo(StreamOperator f) {
        f.linkFrom(this);
        return f;
    }

    abstract public StreamOperator linkFrom(StreamOperator in);

    public StreamOperator linkFrom(StreamOperator in1, StreamOperator in2) {
        List<StreamOperator> ls = new ArrayList();
        ls.add(in1);
        ls.add(in2);
        return linkFrom(ls);
    }

    public StreamOperator linkFrom(StreamOperator in1, StreamOperator in2, StreamOperator in3) {
        List<StreamOperator> ls = new ArrayList();
        ls.add(in1);
        ls.add(in2);
        ls.add(in3);
        return linkFrom(ls);
    }

    public StreamOperator linkFrom(List<StreamOperator> ins) {
        if (null != ins && ins.size() == 1) {
            return linkFrom(ins.get(0));
        } else {
            throw new RuntimeException("Not support more than 1 inputs!");
        }
    }

    public StreamOperator udf(String selectedColName, String outputColName, UserDefinedFunction udf) {
        return linkTo(new UDFStreamOp(selectedColName, outputColName, udf));
    }

    public StreamOperator udf(String selectedColName, String[] outputColNames, UserDefinedFunction udf) {
        return linkTo(new UDFStreamOp(selectedColName, outputColNames, udf));
    }

    public StreamOperator udf(String selectedColName, String outputColName, UserDefinedFunction udf, String[] keepColNames) {
        return linkTo(new UDFStreamOp(selectedColName, outputColName, udf, keepColNames));
    }

    public StreamOperator udf(String selectedColName, String[] outputColNames, UserDefinedFunction udf, String[] keepColNames) {
        return linkTo(new UDFStreamOp(selectedColName, outputColNames, udf, keepColNames));
    }

    public static JobExecutionResult execute() throws Exception {
        return AlinkSession.getStreamExecutionEnvironment().execute();
    }

    public static JobExecutionResult execute(String string) throws Exception {
        return AlinkSession.getStreamExecutionEnvironment().execute(string);
    }

    public static void setParallelism(int parallelism) {
        AlinkSession.getStreamExecutionEnvironment().setParallelism(parallelism);
    }

    public static StreamOperator sql(String query) {
        return new TableSourceStreamOp(AlinkSession.getStreamTableEnvironment().sql(query));
    }

    public StreamOperator select(String param) {
        return linkTo(new SelectStreamOp(param));
    }

    public StreamOperator as(String param) {
        return linkTo(new AsStreamOp(param));
    }

    public StreamOperator where(String param) {
        return linkTo(new WhereStreamOp(param));
    }

    public StreamOperator filter(String param) {
        return linkTo(new FilterStreamOp(param));
    }

    public StreamOperator unionAll(StreamOperator st2) {
        return new UnionAllStreamOp().linkFrom(this, st2);
    }

}
