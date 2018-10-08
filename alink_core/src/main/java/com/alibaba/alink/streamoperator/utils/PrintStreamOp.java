package com.alibaba.alink.streamoperator.utils;

import java.io.PrintStream;

import com.alibaba.alink.common.AlinkParameter;
import com.alibaba.alink.common.utils.RowTypeDataStream;
import com.alibaba.alink.streamoperator.StreamOperator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;


public class PrintStreamOp extends StreamOperator {

    public static void setStreamPrintStream(PrintStream printStream) {
        System.setErr(printStream);
    }

    public PrintStreamOp() {
        super(null);
    }

    public PrintStreamOp(AlinkParameter params) {
        super(params);
    }

    @Override
    public StreamOperator linkFrom(StreamOperator in) {
        try {
            RowTypeDataStream.fromTable(in.getTable()).addSink(new StreamPrintSinkFunction());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        this.table = in.getTable();
        return this;
    }

    public static class StreamPrintSinkFunction extends RichSinkFunction <Row> {
        private static final long serialVersionUID = 1L;
        private transient PrintStream stream;
        private transient String prefix;

        public StreamPrintSinkFunction() {
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            StreamingRuntimeContext context = (StreamingRuntimeContext)this.getRuntimeContext();
            this.stream = System.err;
            this.prefix = context.getNumberOfParallelSubtasks() > 1 ? context.getIndexOfThisSubtask() + 1 + "> " : null;
        }

        public void invoke(Row record) {
            if (this.prefix != null) {
                this.stream.println(this.prefix + record.toString());
            } else {
                this.stream.println(record.toString());
            }

        }

        public void close() {
            this.stream = null;
            this.prefix = null;
        }

        public String toString() {
            return "Print to " + this.stream.toString();
        }
    }
}

