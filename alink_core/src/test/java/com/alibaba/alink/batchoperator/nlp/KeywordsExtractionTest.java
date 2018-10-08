package com.alibaba.alink.batchoperator.nlp;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.batchoperator.source.MemSourceBatchOp;
import com.alibaba.alink.streamoperator.StreamOperator;
import com.alibaba.alink.streamoperator.nlp.KeywordsExtractionStreamOp;
import com.alibaba.alink.streamoperator.source.MemSourceStreamOp;

import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;


public class KeywordsExtractionTest {
    private String text = "翼身融合 飞机 是 未来 航空 领域 发展 一个 新 方向 国内外 诸多 研究 机构 已经 开展 对翼身融合 飞机 研究 而 其 全自动 外形 优化 算法 已 成为 新 研究 热点"
        + " 国内 外 现有 成果 基础 之上 分析 比较 常用 建模 求解 平台 使用 方式 及 特点 设计 编写 翼身融合 飞机 外形 优化 几何 建模 网格 划分 流场 求解 外形 优化 模块 比 较 不同 算法 间 "
        + "优劣 实现 翼身融合 飞机 概念设计 中 外形 优化 几何 建模 及 网格 生成 模块 实现 基于 超限 插值 网格 生成 算法 基于 样条 曲线 建模 方法 流场 求解 模块 包括 有限 差分 求解器 有限元 "
        + "求解器和面元法 求解器 其中 有限 差分 求解器 主要 包括 基于 有限 差分法 势流 数学 建模 基于 笛卡尔 网格 变 步长 差分 格式 推导  笛卡尔 网格 生成 索引 算法 基于 笛卡尔 网格 诺 依曼 "
        + "边界条件 表达 形式 推导 实现 基于 有限 差分 求解器 二维 翼型 气动 参数 计算 算例 有限元 求解器 主要 包括 基于 变分 原理 势流 有限元 理论 建模 二维 有限元 库塔 条件 表达式 推导 基于"
        + " 最小 二乘 速度 求解 算法 设计 基于 Gmsh 二维 带尾迹 翼型 空间 网格 生成器 开发 实现 基于 有限元 求解器 二维 翼型 气动 参数 计算 算例 面元法 求解器 主要 包括 基于 面元法 势流 "
        + "理论 建模 自动 尾迹 生成 算法 设计 基于 面元法 三维 翼身融合 体 流场 求解器 开发 基于 布拉 修斯 平板 解 阻力 估算 算法 设计 求解器 Fortran 语言 上 移 植 Python 和 "
        + "Fortran 代码 混编 基于 OpenMP 和 CUDA 并行 加速 算法 设计 与 开发 实现 基于 面元法 求解器 三维 翼身融合 体 气动 参数 计算 算例 外形 优化 模块 实 现了 基于 自由 形状"
        + " 变形 网格 变形 算法 遗传算法 差分 进化 算法 飞机 表面积 计算 算法 基于 矩 积分 飞 机 体积 计算 算法 开发 基于 VTK 数据 可视化 格式 工具";

    @Test
    public void testGetKeyWordsBatch() {
        Row[] array = new Row[] {Row.of(new Object[] {1, text})};
        MemSourceBatchOp words = new MemSourceBatchOp(Arrays.asList(array), new String[] {"ID", "text"});

        KeywordsExtractionBatchOp evalOp =
            new KeywordsExtractionBatchOp()
                .setDocIdColName("ID")
                .setDocContentColName("text")
                .setTopN(3);

        String[] output = {"1,基于,0.04107086483062794", "1,算法,0.030682828774547973", "1,建模,0.021540575854079275"};
        List <Row> res = evalOp.linkFrom(words).collect();
        String[] results = new String[res.size()];
        for (int i = 0; i < res.size(); i++) {
            results[i] = res.get(i).toString();
        }
        assertArrayEquals(output, results);
    }

    @Test
    public void testGetKeyWordsStream() throws Exception {
        Row[] array = new Row[] {Row.of(new Object[] {1, text})};
        MemSourceStreamOp words = new MemSourceStreamOp(Arrays.asList(array), new String[] {"ID", "text"});

        KeywordsExtractionStreamOp evalOp =
            new KeywordsExtractionStreamOp()
                .setDocIdColName("ID")
                .setDocContentColName("text")
                .setTopN(3)
                .setTimeInterval(1);

        evalOp.linkFrom(words).print();
        StreamOperator.execute();
    }
}
