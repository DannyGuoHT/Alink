package com.alibaba.alink.streamoperator.nlp;

import java.util.Arrays;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.nlp.StringSimilarityConst;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;
import com.alibaba.alink.streamoperator.source.TableSourceStreamOp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

public class StringSimilarityStreamOpTest {
    @Test
    public void test() throws Exception {
        final int interval = 500;
        Row[] arrayTmp =
            new Row[] {
                Row.of(new Object[] {"BG北好啦", "good"}),
                Row.of(new Object[] {"Q是我孩晴北", "dODHxCXiK我"}),
                Row.of(new Object[] {"efWVy8eTm1毕好是帽要服天们气毕", "HFAzrLtSR啦都"}),
                Row.of(new Object[] {"LvAsitnVVE毕", "W3CYyX天吗京电是"}),
                Row.of(new Object[] {"ObfVO吗", "tVsF64jY我好今帽天衣天"}),
                Row.of(new Object[] {"q子我好气衣是", "W们帽电枕头京我头"}),
                Row.of(new Object[] {"JjOboaH椅天天是天服服", "xo啦天服我是子气今"}),
                Row.of(new Object[] {"x8rOWXH4l椅我衣天今衣孩", "JckrXiB6京毕们好毕"}),
                Row.of(new Object[] {"啦椅", "MSCsTVJVT"}),
                Row.of(new Object[] {"BG北好啦", "good"}),
                Row.of(new Object[] {"BG北好啦", "good"}),
                Row.of(new Object[] {"Q是我孩晴北", "dODHxCXiK我"}),
                Row.of(new Object[] {"efWVy8eTm1毕好是帽要服天们气毕", "HFAzrLtSR啦都"}),
                Row.of(new Object[] {"LvAsitnVVE毕", "W3CYyX天吗京电是"}),
                Row.of(new Object[] {"ObfVO吗", "tVsF64jY我好今帽天衣天"}),
                Row.of(new Object[] {"q子我好气衣是", "W们帽电枕头京我头"}),
                Row.of(new Object[] {"JjOboaH椅天天是天服服", "xo啦天服我是子气今"}),
                Row.of(new Object[] {"x8rOWXH4l椅我衣天今衣孩", "JckrXiB6京毕们好毕"}),
                Row.of(new Object[] {"啦椅", "MSCsTVJVT"}),
                Row.of(new Object[] {"BG北好啦", "good"}),
                Row.of(new Object[] {"Q是我孩晴北", "dODHxCXiK我"}),
                Row.of(new Object[] {"efWVy8eTm1毕好是帽要服天们气毕", "HFAzrLtSR啦都"}),
                Row.of(new Object[] {"LvAsitnVVE毕", "W3CYyX天吗京电是"}),
                Row.of(new Object[] {"ObfVO吗", "tVsF64jY我好今帽天衣天"}),
                Row.of(new Object[] {"q子我好气衣是", "W们帽电枕头京我头"}),
                Row.of(new Object[] {"JjOboaH椅天天是天服服", "xo啦天服我是子气今"}),
                Row.of(new Object[] {"x8rOWXH4l椅我衣天今衣孩", "JckrXiB6京毕们好毕"}),
                Row.of(new Object[] {"啦椅", "MSCsTVJVT"}),
                Row.of(new Object[] {"aVPFxk6S头帽气要天我帽", "3DYhqp吗好都子扣天吗帽晴"})
            };

        MemSourceStreamOp array = new MemSourceStreamOp(Arrays.asList(arrayTmp), new String[] {"col0", "col1"});

        TableSourceStreamOp source = new TableSourceStreamOp(array.getDataStream().map(new MapFunction <Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                Thread.sleep(interval);
                return row;
            }
        }), array.getColNames(), array.getColTypes());

        StringSimilarityStreamOp evalOp;

        for (StringSimilarityConst.Method v : StringSimilarityConst.Method.values()) {
            evalOp = new StringSimilarityStreamOp(new AlinkParameter()
                .put("selectedColName0", "col0")
                .put("selectedColName1", "col1")
                .put("method", v.toString())
                .put("outputColName", v.toString())
                .put("timeInterval", 1)
            );
            evalOp.linkFrom(source).print();
        }

        StreamOperator.execute();
    }

}
