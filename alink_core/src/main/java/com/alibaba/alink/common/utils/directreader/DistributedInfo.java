package com.alibaba.alink.common.utils.directreader;

public interface DistributedInfo {
    long startPos(long taskId, long parallelism, long globalRowCnt);
    long localRowCnt(long taskId, long parallelism, long globalRowCnt);
}
