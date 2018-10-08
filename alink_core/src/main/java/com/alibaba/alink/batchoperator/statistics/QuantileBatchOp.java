package com.alibaba.alink.batchoperator.statistics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.alink.batchoperator.BatchOperator;
import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.constants.ParamName;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * In statistics and probability quantiles are cut points dividing
 * the range of a probability distribution into contiguous intervals
 * with equal probabilities, or dividing the observations in a sample
 * in the same way.
 * (https://en.wikipedia.org/wiki/Quantile)
 * <p>
 * reference: Yang, X. (2014). Chong gou da shu ju tong ji (1st ed., pp. 25-29).
 * <p>
 * Note: This algorithm is improved on the base of the parallel
 * sorting by regular sampling(PSRS). The following step is added
 * to the PSRS
 * <ul>
 * <li>replace (val) with (val, task id) to distinguishing the
 * same value on different machines</li>
 * <li>
 * the index of q-quantiles: index = roundMode((n - 1) * k / q),
 * n is the count of sample, k satisfying 0 < k < q
 * </li>
 * </ul>
 *
 */
public class QuantileBatchOp extends BatchOperator {
    public final static int SPLIT_POINT_SIZE = 100000;
    public final static String QUANTILE_NUM = "quantileNum";
    public final static String ROUND_MODE = "roundMode";

    public QuantileBatchOp() {
        super(null);
    }

    public QuantileBatchOp(AlinkParameter params) {
        super(params);
    }

    /**
     * set the name of column which is calculated
     *
     * required
     *
     * @param colName column name of input batch operator
     * @return this
     */
    public QuantileBatchOp setSelectedColName(String colName) {
        params.putIgnoreNull(ParamName.selectedColName, colName);
        return this;
    }

    /**
     * set the size of groups of q-quantiles
     *
     * required
     *
     * @param num the size of groups
     * @return this
     */
    public QuantileBatchOp setQuantileNum(Integer num) {
        params.putIgnoreNull(QUANTILE_NUM, num);
        return this;
    }

    /**
     * set the round mode
     * <p>
     * when
     * q is the group size,
     * k is the k-th group,
     * total is the sample size,
     * then the index of k-th q-quantile is (1.0 / q) * (total - 1) * k.
     * if convert index from double to long, it use round mode.
     *
     * <ul>
     * <li>round: [index]</li>
     * <li>ceil: ⌈index⌉</li>
     * <li>floor: ⌊index⌋</li>
     * </ul>
     *
     * optional
     *
     * @param mode the round mode
     *             <ul>
     *             <li>round</li>
     *             <li>ceil</li>
     *             <li>floor</li>
     *             </ul>
     *             default: round
     * @return this
     */
    public QuantileBatchOp setRoundMode(String mode) {
        params.putIgnoreNull(ROUND_MODE, mode);
        return this;
    }

    @Override
    public BatchOperator linkFrom(BatchOperator in) {

        TableSchema tableSchema = in.getSchema();

        String quantileColName =
            params.getString(ParamName.selectedColName);

        int index = TableUtil.findIndexFromName(tableSchema.getColumnNames(), quantileColName);

        /* filter the selected column from input */
        DataSet <Row> input = in.select(quantileColName).getDataSet();

        /* sort data */
        Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sortedData
            = pSort(input, 0);

        /* calculate quantile */
        DataSet <Row> quantile = sortedData.f0.
            groupBy(0)
            .reduceGroup(new Quantile(
                0, params.getInteger(QUANTILE_NUM),
                params.getStringOrDefault(ROUND_MODE,
                    RoundModeEnum.ROUND.toString())))
            .withBroadcastSet(sortedData.f1, "counts");

        /* set output */
        setTable(quantile,
            new String[] {tableSchema.getColumnNames()[index], "quantile"},
            new TypeInformation <?>[] {tableSchema.getTypes()[index], BasicTypeInfo.LONG_TYPE_INFO});

        return this;
    }

    /**
     *
     */
    public static class Quantile extends RichGroupReduceFunction <Tuple2 <Integer, Row>, Row> {
        private int index;
        private List <Tuple2 <Integer, Long>> counts;
        private long countSum = 0;
        private int quantileNum;
        private String roundType;

        public Quantile(int index, int quantileNum, String roundType) {
            this.index = index;
            this.quantileNum = quantileNum;
            this.roundType = roundType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.counts = getRuntimeContext().getBroadcastVariableWithInitializer(
                "counts",
                new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
                    @Override
                    public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
                        Iterable <Tuple2 <Integer, Long>> data) {
                        // sort the list by task id to calculate the correct offset
                        List <Tuple2 <Integer, Long>> sortedData = new ArrayList <>();
                        for (Tuple2 <Integer, Long> datum : data) {
                            sortedData.add(datum);
                        }
                        Collections.sort(sortedData, new Comparator <Tuple2 <Integer, Long>>() {
                            @Override
                            public int compare(Tuple2 <Integer, Long> o1, Tuple2 <Integer, Long> o2) {
                                return o1.f0.compareTo(o2.f0);
                            }
                        });

                        return sortedData;
                    }
                });

            for (int i = 0; i < this.counts.size(); ++i) {
                countSum += this.counts.get(i).f1;
            }
        }

        @Override
        public void reduce(Iterable <Tuple2 <Integer, Row>> values, Collector <Row> out) throws Exception {
            ArrayList <Row> allRows = new ArrayList <>();
            int id = -1;
            long start = 0;
            long end = 0;

            for (Tuple2 <Integer, Row> value : values) {
                id = value.f0;
                allRows.add(Row.copy(value.f1));
            }

            if (id < 0) {
                throw new Exception("Error key. key: " + id);
            }

            int curListIndex = -1;
            int size = counts.size();

            for (int i = 0; i < size; ++i) {
                int curId = counts.get(i).f0;

                if (curId == id) {
                    curListIndex = i;
                    break;
                }

                if (curId > id) {
                    throw new Exception("Error curId: " + curId
                        + ". id: " + id);
                }

                start += counts.get(i).f1;
            }

            end = start + counts.get(curListIndex).f1;

            if (allRows.size() != end - start) {
                throw new Exception("Error start end."
                    + " start: " + start
                    + ". end: " + end
                    + ". size: " + allRows.size());
            }

            RowComparator rowComparator = new RowComparator(this.index);
            Collections.sort(allRows, rowComparator);

            QIndex qIndex = new QIndex(
                countSum, quantileNum, roundType);

            for (int i = 0; i <= quantileNum; ++i) {
                long index = qIndex.genIndex(i);

                if (index >= start && index < end) {
                    out.collect(
                        RowUtil.merge(allRows.get((int)(index - start)), Long.valueOf(i)));
                }
            }
        }

    }

    /**
     *
     */
    public static class SampleSplitPoint extends RichMapPartitionFunction <Row, Tuple2 <Object, Integer>> {
        private int taskId;
        private int index;

        public SampleSplitPoint(int index) {
            this.index = index;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            this.taskId = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Object, Integer>> out) throws Exception {
            ArrayList <Object> allValues = new ArrayList <>();

            for (Row row : values) {
                allValues.add(row.getField(index));
            }

            if (allValues.isEmpty()) {
                return;
            }

            Collections.sort(allValues, new ObjectComparator());

            int size = allValues.size();

            Set <Object> splitPoints = new HashSet <>();

            for (int i = 0; i < SPLIT_POINT_SIZE; ++i) {
                int index = genSampleIndex(
                    Long.valueOf(i),
                    Long.valueOf(size),
                    Long.valueOf(SPLIT_POINT_SIZE))
                    .intValue();

                if (index >= size) {
                    throw new Exception("Index error. index: " + index + ". totalCount: " + size);
                }

                splitPoints.add(allValues.get(index));
            }
            for (Object obj : splitPoints) {
                Tuple2 <Object, Integer> cur
                    = new Tuple2 <Object, Integer>(
                    obj,
                    taskId);

                out.collect(cur);
            }
        }
    }

    public static class SplitPointReducer
        extends RichGroupReduceFunction <Tuple2 <Object, Integer>, Tuple2 <Object, Integer>> {

        private int instanceCount;

        public SplitPointReducer() {}

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            instanceCount = getRuntimeContext().getNumberOfParallelSubtasks();
        }

        @Override
        public void reduce(
            Iterable <Tuple2 <Object, Integer>> values,
            Collector <Tuple2 <Object, Integer>> out) throws Exception {
            ArrayList <Tuple2 <Object, Integer>> all = new ArrayList <>();

            for (Tuple2 <Object, Integer> value : values) {
                all.add(new Tuple2 <>(value.f0, value.f1));
            }

            int count = all.size();

            Collections.sort(all, new PairComparator());

            Set <Tuple2 <Object, Integer>> spliters = new HashSet <>();

            int splitPointSize = instanceCount - 1;
            for (int i = 0; i < splitPointSize; ++i) {
                int index = genSampleIndex(
                    Long.valueOf(i),
                    Long.valueOf(count),
                    Long.valueOf(splitPointSize))
                    .intValue();

                if (index >= count) {
                    throw new Exception("Index error. index: " + index + ". totalCount: " + count);
                }

                spliters.add(all.get(index));
            }

            for (Tuple2 <Object, Integer> spliter : spliters) {
                out.collect(spliter);
            }
        }
    }

    public static class SplitData extends RichMapPartitionFunction <Row, Tuple2 <Integer, Row>> {
        private int taskId;
        private List <Tuple2 <Object, Integer>> splitPoints;
        private int index;

        public SplitData(int index) {
            this.index = index;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RuntimeContext ctx = getRuntimeContext();

            this.taskId = ctx.getIndexOfThisSubtask();
            this.splitPoints = ctx.getBroadcastVariableWithInitializer(
                "splitPoints",
                new BroadcastVariableInitializer <Tuple2 <Object, Integer>, List <Tuple2 <Object, Integer>>>() {
                    @Override
                    public List <Tuple2 <Object, Integer>> initializeBroadcastVariable(
                        Iterable <Tuple2 <Object, Integer>> data) {
                        // sort the list by task id to calculate the correct offset
                        List <Tuple2 <Object, Integer>> sortedData = new ArrayList <>();
                        for (Tuple2 <Object, Integer> datum : data) {
                            sortedData.add(datum);
                        }
                        Collections.sort(sortedData, new PairComparator());
                        return sortedData;
                    }
                });
        }

        @Override
        public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) throws Exception {
            if (splitPoints.isEmpty()) {
                for (Row row : values) {
                    out.collect(new Tuple2 <>(0, row));
                }

                return;
            }

            ArrayList <Row> allRows = new ArrayList <>();
            ArrayList <Integer> allRowsIndex = new ArrayList <>();

            int size = 0;
            for (Row row : values) {
                allRows.add(Row.copy(row));
                allRowsIndex.add(size);
                size++;
            }

            IndexedObjectComparator indexedObjectComparator
                = new IndexedObjectComparator(allRows, index);
            Collections.sort(allRowsIndex, indexedObjectComparator);

            int spliterSize = splitPoints.size();
            int curIndex = 0;

            PairComparator pairComparator = new PairComparator();
            Tuple2 <Object, Integer> curTuple = new Tuple2 <Object, Integer>(
                null,
                taskId
            );
            for (int i = 0; i < size; ++i) {
                Row curRow = allRows.get(allRowsIndex.get(i));
                curTuple.f0 = curRow.getField(index);

                int code = pairComparator.compare(
                    curTuple,
                    splitPoints.get(curIndex)
                );

                if (code > 0) {
                    ++curIndex;

                    while (curIndex < spliterSize && code > 0) {
                        code = pairComparator.compare(
                            curTuple,
                            splitPoints.get(curIndex));
                        ++curIndex;
                    }
                }

                if (curIndex >= spliterSize) {
                    for (int j = i; j < size; ++j) {
                        out.collect(new Tuple2 <>(curIndex, allRows.get(allRowsIndex.get(j))));
                    }

                    break;
                }
                out.collect(new Tuple2 <>(curIndex, curRow));
            }
        }
    }

    public interface RoundType {
        long calc(double a);
    }

    public enum RoundModeEnum implements RoundType {
        /* ⌈a⌉ */
        CEIL(new RoundType() {
            @Override
            public long calc(double a) {
                return (long)Math.ceil(a);
            }
        }),

        /* ⌊a⌋ */
        FLOOR(new RoundType() {
            @Override
            public long calc(double a) {
                return (long)Math.floor(a);
            }
        }),

        /* [a] */
        ROUND(new RoundType() {
            @Override
            public long calc(double a) {
                return Math.round(a);
            }
        });

        private final RoundType roundType;

        RoundModeEnum(RoundType roundType) {
            this.roundType = roundType;
        }

        @Override
        public String toString() {
            return super.name();
        }

        @Override
        public long calc(double a) {
            /**
             * 0.1 * (8.0 - 1.0) * 10.0 = 7.000000000000001,
             * we hold 14 digits after the decimal point to avoid this situation
             */
            BigDecimal bigDecimal = new BigDecimal(a);
            return roundType.calc(bigDecimal.setScale(14,
                BigDecimal.ROUND_HALF_UP).doubleValue());
        }

        public static <T extends Enum <T>> T of(Class <T> enumType, String val) {
            for (T typeEnum : enumType.getEnumConstants()) {
                if (typeEnum.toString().toLowerCase().equals(val.trim().toLowerCase())) {
                    return typeEnum;
                }
            }

            return null;
        }
    }

    public static class QIndex {
        private double totalCount;
        private double q1;
        private RoundModeEnum roundMode;

        public QIndex(double totalCount, int quantileNum, String type) {
            this.totalCount = totalCount;
            this.q1 = 1.0 / (double)quantileNum;
            this.roundMode = RoundModeEnum.of(RoundModeEnum.class, type);
        }

        public long genIndex(int k) {
            return roundMode.calc(this.q1 * (this.totalCount - 1.0) * (double)k);
        }
    }

    public static class ObjectComparator implements Comparator <Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            }

            Comparable c0 = (Comparable)o1;
            Comparable c1 = (Comparable)o2;

            return c0.compareTo(c1);
        }
    }

    public static class IndexedObjectComparator
        implements Comparator <Integer> {
        private List <Row> listObject;
        private ObjectComparator objectComparator = new ObjectComparator();
        private int index;

        public IndexedObjectComparator(List listObject, int index) {
            this.listObject = listObject;
            this.index = index;
        }

        @Override
        public int compare(Integer o1, Integer o2) {
            return objectComparator.compare(
                listObject.get(o1).getField(index),
                listObject.get(o2).getField(index));
        }
    }

    public static class RowComparator implements Comparator <Row> {
        private ObjectComparator objectComparator = new ObjectComparator();
        private int index;

        public RowComparator(int index) {
            this.index = index;
        }

        @Override
        public int compare(Row o1, Row o2) {
            return objectComparator.compare(
                o1.getField(index),
                o2.getField(index));
        }
    }

    public static class PairComparator
        implements Comparator <Tuple2 <Object, Integer>> {
        ObjectComparator objectComparator = new ObjectComparator();

        @Override
        public int compare(Tuple2 <Object, Integer> o1, Tuple2 <Object, Integer> o2) {
            int ret = objectComparator.compare(o1.f0, o2.f0);
            if (ret == 0) {
                return o1.f1.compareTo(o2.f1);
            }

            return ret;
        }
    }

    public static Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> pSort(
        DataSet <Row> input, int index) {

        DataSet <Tuple2 <Object, Integer>> splitPoints = input.mapPartition(new SampleSplitPoint(index))
            .reduceGroup(new SplitPointReducer());

        DataSet <Tuple2 <Integer, Row>> splitData = input
            .mapPartition(new SplitData(index))
            .withBroadcastSet(splitPoints, "splitPoints");

        DataSet <Tuple2 <Integer, Long>> allCount = splitData
            .groupBy(0)
            .reduceGroup(new GroupReduceFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Long>>() {
                @Override
                public void reduce(
                    Iterable <Tuple2 <Integer, Row>> values,
                    Collector <Tuple2 <Integer, Long>> out) {
                    Integer id = -1;
                    Long count = 0L;
                    for (Tuple2 <Integer, Row> value : values) {
                        id = value.f0;
                        count++;
                    }

                    out.collect(new Tuple2 <>(id, count));
                }
            });

        return new Tuple2 <>(splitData, allCount);
    }

    public static Long genSampleIndex(Long splitPointIdx, Long count, Long splitPointSize) {
        Long div = count / (splitPointSize + 1);
        Long mod = count % (splitPointSize + 1);

        return div * (splitPointIdx + 1)
            + ((mod > (splitPointIdx + 1)) ? (splitPointIdx + 1) : mod)
            - 1;
    }
}
