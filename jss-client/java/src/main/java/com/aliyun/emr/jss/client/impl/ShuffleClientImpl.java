package com.aliyun.emr.jss.client.impl;

import com.aliyun.emr.jss.client.ShuffleClient;
import com.aliyun.emr.jss.common.JindoConf;
import com.aliyun.emr.jss.common.rpc.RpcAddress;
import com.aliyun.emr.jss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.jss.common.rpc.RpcEnv;
import com.aliyun.emr.jss.common.util.Utils;
import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.protocol.RpcNameConstants;
import com.aliyun.emr.jss.protocol.message.ShuffleMessages;
import scala.reflect.ClassTag$;

import java.io.InputStream;
import java.util.List;

public class ShuffleClientImpl extends ShuffleClient
{
    private RpcEnv _env = null;
    private RpcEndpointRef _master = null;
    public void init() {
        String localhost = Utils.localHostName();
        _env = RpcEnv.create("ShuffleClient",
            localhost,
            0,
            new JindoConf(), true);
        _master = _env.setupEndpointRef(new RpcAddress(localhost, 9099), RpcNameConstants.MASTER_EP);
    }

    @Override
    public List<PartitionLocation> registerShuffle(String applicationId, int shuffleId, int numMappers, int numPartitions)
    {
        ShuffleMessages.RegisterResponse response = _master.askSync(
            new ShuffleMessages.Register(applicationId, shuffleId, numMappers, numPartitions),
            ClassTag$.MODULE$.apply(ShuffleMessages.RegisterResponse.class)
        );
        if (response.success()) {
            return response.partitionLocations();
        } else {
            return null;
        }
    }

    @Override
    public void pushData(byte[] data, PartitionLocation location)
    {

    }

    private PartitionLocation revive(int mapperId, PartitionLocation location)
    {
        return null;
    }

    @Override
    public void unregisterShuffle(String applicationId, int shuffleId)
    {

    }

    @Override
    public InputStream readPartition(String applicationId, int shuffleId, int reduceId)
    {
        return null;
    }
}
