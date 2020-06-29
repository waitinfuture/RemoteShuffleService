package com.aliyun.emr.ess.client;

public interface MetricsCallback {
    void incBytesWritten(long bytesWritten);
    void incReadTime(long time);
}
