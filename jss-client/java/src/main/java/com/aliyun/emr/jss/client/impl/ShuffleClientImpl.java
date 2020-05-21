package com.aliyun.emr.jss.client.impl;

import com.aliyun.emr.jss.client.ShuffleClient;
import com.aliyun.emr.jss.common.EssConf;
import com.aliyun.emr.jss.common.rpc.RpcAddress;
import com.aliyun.emr.jss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.jss.common.rpc.RpcEnv;
import com.aliyun.emr.jss.common.util.Utils;
import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.protocol.RpcNameConstants;
import com.aliyun.emr.jss.protocol.message.ControlMessages.*;
import scala.reflect.ClassTag$;

import java.io.InputStream;
import java.util.List;

public class ShuffleClientImpl extends ShuffleClient {
    private RpcEnv _env = null;
    private RpcEndpointRef _master = null;
    public void init() {
        String localhost = Utils.localHostName();
        _env = RpcEnv.create("ShuffleClient",
            localhost,
            0,
            new EssConf());
        _master = _env.setupEndpointRef(new RpcAddress(localhost, 9099), RpcNameConstants.MASTER_EP);
    }

    @Override
    public List<PartitionLocation> registerShuffle(String applicationId, int shuffleId, int numMappers, int numPartitions) {
        RegisterShuffleResponse response = _master.askSync(
            new RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions),
            ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)
        );
        if (response.success()) {
            return response.partitionLocations();
        } else {
            return null;
        }
    }

    @Override
    public PartitionLocation revive(String applicationId, int shuffleId) {
        ReviveResponse response = _master.askSync(
            new Revive(applicationId, shuffleId),
            ClassTag$.MODULE$.apply(ReviveResponse.class)
        );
        if (response.success()) {
            return response.newLocation();
        } else {
            return null;
        }
    }

    @Override
    public void pushData(byte[] data, PartitionLocation location) {

    }

    private PartitionLocation revive(int mapperId, PartitionLocation location)
    {
        return null;
    }

    @Override
    public void unregisterShuffle(String applicationId, int shuffleId) {

    }

    @Override
    public InputStream readPartition(String applicationId, int shuffleId, int reduceId)
    {
        return null;
    }
}
