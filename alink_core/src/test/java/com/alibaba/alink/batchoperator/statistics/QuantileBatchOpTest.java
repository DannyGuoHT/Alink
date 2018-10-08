package com.alibaba.alink.batchoperator.statistics;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;

import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;


public class QuantileBatchOpTest {
    MemSourceBatchOp sourceBatchOp;

    @Test
    public void linkFromString() {
        QuantileBatchOp quantileBatchOp
            = new QuantileBatchOp()
            .setSelectedColName("col1")
            .setQuantileNum(10);

        List <Row> result = quantileBatchOp.linkFrom(sourceBatchOp).collect();

        Row[] expect =
            new Row[] {
                Row.of(new Object[] {"a", 0L}),
                Row.of(new Object[] {"a", 1L}),
                Row.of(new Object[] {"a", 2L}),
                Row.of(new Object[] {"b", 3L}),
                Row.of(new Object[] {"b", 4L}),
                Row.of(new Object[] {"c", 5L}),
                Row.of(new Object[] {"c", 6L}),
                Row.of(new Object[] {"c", 7L}),
                Row.of(new Object[] {"d", 8L}),
                Row.of(new Object[] {"d", 9L}),
                Row.of(new Object[] {"d", 10L})
            };
        System.out.println(result);
        Assert.assertThat(result, is(Arrays.asList(expect)));
    }

    @Test
    public void linkFromInt() {
        QuantileBatchOp quantileBatchOp
            = new QuantileBatchOp()
            .setSelectedColName("col2")
            .setQuantileNum(10);

        List <Row> result = quantileBatchOp.linkFrom(sourceBatchOp).collect();
        /**
         * -99,-99,-2,-2,1,1,100,100
         */
        Row[] expect =
            new Row[] {
                Row.of(new Object[] {-99, 0L}),
                Row.of(new Object[] {-99, 1L}),
                Row.of(new Object[] {-99, 2L}),
                Row.of(new Object[] {-2, 3L}),
                Row.of(new Object[] {-2, 4L}),
                Row.of(new Object[] {1, 5L}),
                Row.of(new Object[] {1, 6L}),
                Row.of(new Object[] {1, 7L}),
                Row.of(new Object[] {100, 8L}),
                Row.of(new Object[] {100, 9L}),
                Row.of(new Object[] {100, 10L})
            };
        System.out.println(result);
        Assert.assertThat(result, is(Arrays.asList(expect)));
    }

    @Test
    public void linkFromDouble() throws Exception {
        QuantileBatchOp quantileBatchOp
            = new QuantileBatchOp()
            .setSelectedColName("col3")
            .setQuantileNum(10);

        List <Row> result = quantileBatchOp.linkFrom(sourceBatchOp).collect();
        /**
         * -0.01,-0.01,0.9,0.9,1.1,1.1,100.9,100.9
         */
        Row[] expect =
            new Row[] {
                Row.of(new Object[] {-0.01, 0L}),
                Row.of(new Object[] {-0.01, 1L}),
                Row.of(new Object[] {-0.01, 2L}),
                Row.of(new Object[] {0.9, 3L}),
                Row.of(new Object[] {0.9, 4L}),
                Row.of(new Object[] {1.1, 5L}),
                Row.of(new Object[] {1.1, 6L}),
                Row.of(new Object[] {1.1, 7L}),
                Row.of(new Object[] {100.9, 8L}),
                Row.of(new Object[] {100.9, 9L}),
                Row.of(new Object[] {100.9, 10L})
            };
        System.out.println(result);
        Assert.assertThat(result, is(Arrays.asList(expect)));
    }

    @Before
    public void setUp() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of(new Object[] {"a", 1, 1.1}),
                Row.of(new Object[] {"b", -2, 0.9}),
                Row.of(new Object[] {"c", 100, -0.01}),
                Row.of(new Object[] {"d", -99, 100.9}),
                Row.of(new Object[] {"a", 1, 1.1}),
                Row.of(new Object[] {"b", -2, 0.9}),
                Row.of(new Object[] {"c", 100, -0.01}),
                Row.of(new Object[] {"d", -99, 100.9})
            };

        String[] colnames = new String[] {"col1", "col2", "col3"};
        sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void setRoundModeCeil() {
        QuantileBatchOp quantileBatchOp
            = new QuantileBatchOp()
            .setSelectedColName("col3")
            .setQuantileNum(10)
            .setRoundMode("CEIL");

        List <Row> result = quantileBatchOp.linkFrom(sourceBatchOp).collect();
        /**
         * -0.01,-0.01,0.9,0.9,1.1,1.1,100.9,100.9
         */
        Row[] expect =
            new Row[] {
                Row.of(new Object[] {-0.01, 0L}),
                Row.of(new Object[] {-0.01, 1L}),
                Row.of(new Object[] {0.9, 2L}),
                Row.of(new Object[] {0.9, 3L}),
                Row.of(new Object[] {0.9, 4L}),
                Row.of(new Object[] {1.1, 5L}),
                Row.of(new Object[] {1.1, 6L}),
                Row.of(new Object[] {1.1, 7L}),
                Row.of(new Object[] {100.9, 8L}),
                Row.of(new Object[] {100.9, 9L}),
                Row.of(new Object[] {100.9, 10L})
            };
        System.out.println(result);
        Assert.assertThat(result, is(Arrays.asList(expect)));
    }

    @Test
    public void setRoundModeFloor() {
        QuantileBatchOp quantileBatchOp
            = new QuantileBatchOp()
            .setSelectedColName("col3")
            .setQuantileNum(10)
            .setRoundMode("floor");

        List <Row> result = quantileBatchOp.linkFrom(sourceBatchOp).collect();
        /**
         * -0.01,-0.01,0.9,0.9,1.1,1.1,100.9,100.9
         */
        Row[] expect =
            new Row[] {
                Row.of(new Object[] {-0.01, 0L}),
                Row.of(new Object[] {-0.01, 1L}),
                Row.of(new Object[] {-0.01, 2L}),
                Row.of(new Object[] {0.9, 3L}),
                Row.of(new Object[] {0.9, 4L}),
                Row.of(new Object[] {0.9, 5L}),
                Row.of(new Object[] {1.1, 6L}),
                Row.of(new Object[] {1.1, 7L}),
                Row.of(new Object[] {1.1, 8L}),
                Row.of(new Object[] {100.9, 9L}),
                Row.of(new Object[] {100.9, 10L})
            };
        System.out.println(result);
        Assert.assertThat(result, is(Arrays.asList(expect)));
    }

    @Test
    public void genSampleIndex() {
        Assert.assertEquals(Long.valueOf(14L), QuantileBatchOp.genSampleIndex(10L, 20L, 15L));
        /* out of bound. bound: mod */
        Assert.assertEquals(Long.valueOf(24L), QuantileBatchOp.genSampleIndex(20L, 20L, 15L));
    }
}