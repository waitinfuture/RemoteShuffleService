package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.network.protocol.PushData;
import com.aliyun.emr.network.client.RpcResponseCallback;

public interface PushDataHandler {
    void handlePushData(PushData pushData, RpcResponseCallback callback);
}
