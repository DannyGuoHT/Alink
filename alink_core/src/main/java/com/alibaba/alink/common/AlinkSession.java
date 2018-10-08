package com.alibaba.alink.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class AlinkSession {

    public static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static ExecutionEnvironment env;
    private static StreamExecutionEnvironment streamEnv;
    private static BatchTableEnvironment batchTableEnv;
    private static StreamTableEnvironment streamTableEnv;

    public static ExecutionEnvironment getExecutionEnvironment() {
        if (null == env) {
            env = ExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
    }

    public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        if (null == streamEnv) {
            streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return streamEnv;
    }

    public static BatchTableEnvironment getBatchTableEnvironment() {
        if (null == batchTableEnv) {
            batchTableEnv = TableEnvironment.getTableEnvironment(getExecutionEnvironment());
        }
        return batchTableEnv;
    }

    public static StreamTableEnvironment getStreamTableEnvironment() {
        if (null == streamTableEnv) {
            streamTableEnv = TableEnvironment.getTableEnvironment(getStreamExecutionEnvironment());
        }
        return streamTableEnv;
    }

}

