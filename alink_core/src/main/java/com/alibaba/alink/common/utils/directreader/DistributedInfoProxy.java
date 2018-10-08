package com.alibaba.alink.common.utils.directreader;

public class DistributedInfoProxy {
    DistributedInfo distributedInfo;
    private long taskId;
    private long parallelism;

    public DistributedInfoProxy(DistributedInfo distributedInfo, long taskId, long parallelism) {
        this.distributedInfo = distributedInfo;
        this.taskId = taskId;
        this.parallelism = parallelism;
    }

    public long startPos(long globalRowCnt) {
        return distributedInfo.startPos(taskId, parallelism, globalRowCnt);
    }

    public long localRowCnt(long globalRowCnt){
        return distributedInfo.localRowCnt(taskId, parallelism, globalRowCnt);
    };
}
