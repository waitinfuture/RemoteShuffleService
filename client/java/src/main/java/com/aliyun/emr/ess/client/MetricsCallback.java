package com.aliyun.emr.ess.client;

public interface MetricsCallback {
    void incBytesRead(long bytesRead);
    void incReadTime(long time);
}
