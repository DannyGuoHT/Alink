package com.alibaba.alink.batchoperator;

import com.alibaba.alink.batchoperator.dataproc.CrossBatchOp;
import com.alibaba.alink.batchoperator.dataproc.FirstN;
import com.alibaba.alink.batchoperator.dataproc.SampleBatchOp;
import com.alibaba.alink.batchoperator.dataproc.SampleWithSizeBatchOp;
import com.alibaba.alink.batchoperator.source.TableSourceBatchOp;
import com.alibaba.alink.batchoperator.sql.*;
import com.alibaba.alink.batchoperator.utils.PrintBatchOp;
import com.alibaba.alink.batchoperator.utils.udf.UDFBatchOp;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.AlinkSession;
import com.alibaba.alink.common.utils.RowTypeDataSet;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.plan.schema.RelTable;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public abstract class BatchOperator {

    protected AlinkParameter params;

    protected Table table = null;

    private String tableName = null;

    protected Table[] sideTables = null;

    protected BatchOperator(AlinkParameter params) {
        if (null == params) {
            this.params = new AlinkParameter();
        } else {
            this.params = params.clone();
        }
    }

    public AlinkParameter getParams() {
        return this.params.clone();
    }

    protected void putParamValue(String paramName, Object paramValue) {
        if (null == this.params) {
            this.params = new AlinkParameter();
        }
        this.params.put(paramName, paramValue);
    }

    public BatchOperator param(String paramName, Object paramValue) {
        putParamValue(paramName, paramValue);
        return this;
    }

    public long count() throws Exception {
        return RowTypeDataSet.fromTable(this.table).count();
    }

    public DataSet <Row> getDataSet() {
        return RowTypeDataSet.fromTable(this.table);
    }

    protected void setTable(DataSet <Row> dataSet, TableSchema schema) {
        this.table = RowTypeDataSet.toTable(dataSet, schema);
    }

    protected void setTable(DataSet <Row> dataSet) {
        this.table = RowTypeDataSet.toTable(dataSet);
    }

    protected void setTable(DataSet <Row> dataSet, String[] colNames) {
        this.table = RowTypeDataSet.toTable(dataSet, colNames);
    }

    protected void setTable(DataSet <Row> dataSet, String[] colNames, TypeInformation <?>[] colTypes) {
        this.table = RowTypeDataSet.toTable(dataSet, colNames, colTypes);
    }

    public List <Row> collect() {
        try {
            return getDataSet().collect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
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

    public BatchOperator setTableName(String name) {
        AlinkSession.getBatchTableEnvironment().registerTable(name, this.table);
        this.tableName = name;
        return this;
    }

    public BatchOperator forceSetTableName(String name) {
        if (AlinkSession.getBatchTableEnvironment().isRegistered(name)) {
            AlinkSession.getBatchTableEnvironment().replaceRegisteredTable(name, new RelTable(this.table.getRelNode()));
        } else {
            AlinkSession.getBatchTableEnvironment().registerTable(name, this.table);
        }
        this.tableName = name;
        return this;
    }

    public static String createUniqueTableName() {
        return TableUtil.getTempTableName();
    }

    public TypeInformation <?>[] getColTypes() {
        return getSchema().getTypes();
    }

    public String[] getColNames() {
        return getSchema().getColumnNames();
    }

    public TableSchema getSchema() {
        return this.table.getSchema();
    }

    public BatchOperator getSideOutput() {
        return getSideOutput(0);
    }

    public BatchOperator getSideOutput(int idx) {
        if (null == this.sideTables) {
            throw new RuntimeException("There is no side output.");
        } else if (idx < 0 && idx >= this.sideTables.length) {
            throw new RuntimeException("There is no  side output.");
        } else {
            return new TableSourceBatchOp(this.sideTables[idx]);
        }
    }

    public int getSideOutputCount() {
        return null == this.sideTables ? 0 : this.sideTables.length;
    }

    public BatchOperator print() throws Exception {
        return linkTo(new PrintBatchOp());
    }

    public BatchOperator sample(double ratio) {
        return linkTo(new SampleBatchOp(ratio));
    }

    public BatchOperator sample(double ratio, boolean withReplacement) {
        return linkTo(new SampleBatchOp(ratio, withReplacement));
    }

    public BatchOperator sample(double ratio, boolean withReplacement, long seed) {
        return linkTo(new SampleBatchOp(ratio, withReplacement, seed));
    }

    public BatchOperator sampleWithSize(int numSamples) {
        return linkTo(new SampleWithSizeBatchOp(numSamples));
    }

    public BatchOperator sampleWithSize(int numSamples, boolean withReplacement) {
        return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement));
    }

    public BatchOperator sampleWithSize(int numSamples, boolean withReplacement, long seed) {
        return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement, seed));
    }

    @Override
    public String toString() {
        return this.table.toString();
    }

    public BatchOperator link(BatchOperator f) {
        return linkTo(f);
    }

    public BatchOperator linkTo(BatchOperator f) {
        f.linkFrom(this);
        return f;
    }

    abstract public BatchOperator linkFrom(BatchOperator in);

    public BatchOperator linkFrom(BatchOperator in1, BatchOperator in2) {
        List <BatchOperator> ls = new ArrayList();
        ls.add(in1);
        ls.add(in2);
        return linkFrom(ls);
    }

    public BatchOperator linkFrom(BatchOperator in1, BatchOperator in2, BatchOperator in3) {
        List <BatchOperator> ls = new ArrayList();
        ls.add(in1);
        ls.add(in2);
        ls.add(in3);
        return linkFrom(ls);
    }

    public BatchOperator linkFrom(List <BatchOperator> ins) {
        if (null != ins && ins.size() == 1) {
            return linkFrom(ins.get(0));
        } else {
            throw new RuntimeException("Not support more than 1 inputs!");
        }
    }

    public BatchOperator udf(String selectColName, String newColName, UserDefinedFunction udf) {
        return linkTo(new UDFBatchOp(selectColName, newColName, udf));
    }

    public BatchOperator udf(String selectColName, String[] newColNames, UserDefinedFunction udf) {
        return linkTo(new UDFBatchOp(selectColName, newColNames, udf));
    }

    public BatchOperator udf(String selectColName, String newColName, UserDefinedFunction udf,
                             String[] keepOldColNames) {
        return linkTo(new UDFBatchOp(selectColName, newColName, udf, keepOldColNames));
    }

    public BatchOperator udf(String selectColName, String[] newColNames, UserDefinedFunction udf,
                             String[] keepOldColNames) {
        return linkTo(new UDFBatchOp(selectColName, newColNames, udf, keepOldColNames));
    }

    public static BatchOperator sql(String query) {
        return new TableSourceBatchOp(AlinkSession.getBatchTableEnvironment().sql(query));
    }

    public FirstN firstN(int n) {
        return (FirstN)linkTo(new FirstN(n));
    }

    public BatchOperator cross(BatchOperator batchOp) {
        CrossBatchOp crs = new CrossBatchOp();
        return crs.linkFrom(this, batchOp);
    }

    public BatchOperator crossWithTiny(BatchOperator batchOp) {
        CrossBatchOp crs = new CrossBatchOp(CrossBatchOp.Type.WithTiny);
        return crs.linkFrom(this, batchOp);
    }

    public BatchOperator crossWithHuge(BatchOperator batchOp) {
        CrossBatchOp crs = new CrossBatchOp(CrossBatchOp.Type.WithHuge);
        return crs.linkFrom(this, batchOp);
    }


    public BatchOperator select(String param) {
        return linkTo(new SelectBatchOp(param));
    }

    public BatchOperator as(String param) {
        return linkTo(new AsBatchOp(param));
    }

    public BatchOperator where(String param) {
        return linkTo(new WhereBatchOp(param));
    }

    public BatchOperator filter(String param) {
        return linkTo(new FilterBatchOp(param));
    }

    public BatchOperator distinct() {
        return linkTo(new DistinctBatchOp());
    }

    public BatchOperator orderBy(String param) {
        return linkTo(new OrderByBatchOp(param));
    }

    public BatchOperator orderBy(String param, int limit) {
        return linkTo(new OrderByBatchOp(param, limit));
    }

    public BatchOperator orderBy(String param, int offset, int fetch) {
        return linkTo(new OrderByBatchOp(param, offset, fetch));
    }

    public BatchOperator groupBy(String clauseGroupBy, String clauseSelect) {
        return linkTo(new GroupByBatchOp(clauseGroupBy, clauseSelect));
    }

    public BatchOperator join(BatchOperator table2, String joinPredicate) {
        return join(table2, joinPredicate, null);
    }

    public BatchOperator join(BatchOperator table2, String joinPredicate, String selectClause) {
        return new JoinBatchOp(joinPredicate, selectClause).linkFrom(this, table2);
    }

    public BatchOperator leftOuterJoin(BatchOperator table2, String joinPredicate) {
        return leftOuterJoin(table2, joinPredicate, null);
    }

    public BatchOperator leftOuterJoin(BatchOperator table2, String joinPredicate, String selectClause) {
        return new LeftOuterJoinBatchOp(joinPredicate, selectClause).linkFrom(this, table2);
    }

    public BatchOperator rightOuterJoin(BatchOperator table2, String joinPredicate) {
        return rightOuterJoin(table2, joinPredicate, null);
    }

    public BatchOperator rightOuterJoin(BatchOperator table2, String joinPredicate, String selectClause) {
        return new RightOuterJoinBatchOp(joinPredicate, selectClause).linkFrom(this, table2);
    }

    public BatchOperator fullOuterJoin(BatchOperator table2, String joinPredicate) {
        return fullOuterJoin(table2, joinPredicate, null);
    }

    public BatchOperator fullOuterJoin(BatchOperator table2, String joinPredicate, String selectClause) {
        return new FullOuterJoinBatchOp(joinPredicate, selectClause).linkFrom(this, table2);
    }

    public BatchOperator union(BatchOperator table2) {
        return new UnionBatchOp().linkFrom(this, table2);
    }

    public BatchOperator unionAll(BatchOperator table2) {
        return new UnionAllBatchOp().linkFrom(this, table2);
    }

    public BatchOperator intersect(BatchOperator table2) {
        return new IntersectBatchOp().linkFrom(this, table2);
    }

    public BatchOperator intersectAll(BatchOperator table2) {
        return new IntersectAllBatchOp().linkFrom(this, table2);
    }

    public BatchOperator minus(BatchOperator table2) {
        return new MinusBatchOp().linkFrom(this, table2);
    }

    public BatchOperator minusAll(BatchOperator table2) {
        return new MinusAllBatchOp().linkFrom(this, table2);
    }

    public static void execute() throws Exception {
        AlinkSession.getExecutionEnvironment().execute();
    }

    public static void setParallelism(int parallelism) {
        AlinkSession.getExecutionEnvironment().setParallelism(parallelism);
    }

    public static void disableLogging() {
        AlinkSession.getExecutionEnvironment().getConfig().disableSysoutLogging();
    }

}
