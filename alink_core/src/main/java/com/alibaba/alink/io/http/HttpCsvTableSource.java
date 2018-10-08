/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.io.http;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class HttpCsvTableSource implements BatchTableSource<Row>, StreamTableSource<Row> {
    private String path;
    private TypeInformation<?>[] fieldTypes;
    private String[] fieldNames;
    private String fieldDelim;
    private String rowDelim;
    private boolean ignoreFirstLine;


    public HttpCsvTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes,
                              String fieldDelim, String rowDelim, Character quoteCharacter,
                              boolean ignoreFirstLine, String ignoreComments, boolean lenient) {
        if (!rowDelim.equals("\n"))
            throw new RuntimeException("Not support row delimiter other than \"\\n\"");
        if (quoteCharacter != null || ignoreComments != null || lenient)
            throw new RuntimeException("Not implemented quoteCharacter/ignoreComments/lenient");

        this.path = path;
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.fieldDelim = fieldDelim;
        this.rowDelim = rowDelim;
        this.ignoreFirstLine = ignoreFirstLine;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    /**
     * NOTE: This interface is not present in Blink's version of TableSource,
     * but present in Flink-1.4.0. So I drop the @Override annotation so that
     * it compiles for both blink and flink.
     */
    public TableSchema getTableSchema() {
        return new TableSchema(fieldNames, fieldTypes);
    }

    @Override
    public String explainSource() {
        return "HttpCsvTableSource";
    }

    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
        return execEnv.createInput(createInputFormat(), getReturnType());
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecEnv) {
        return streamExecEnv.createInput(createInputFormat(), getReturnType());
    }

    private HttpRowCsvInputFormat createInputFormat() {
        return new HttpRowCsvInputFormat(path, fieldTypes, fieldDelim, rowDelim, ignoreFirstLine);
    }
}
