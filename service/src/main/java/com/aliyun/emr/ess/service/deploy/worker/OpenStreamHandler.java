package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.network.server.FileInfo;

public interface OpenStreamHandler {

    FileInfo handleOpenStream(String shuffleKey, String partitionId);
}
