package com.aliyun.emr.jss.client;

import com.aliyun.emr.jss.protocol.PartitionLocation;

import java.io.InputStream;
import java.util.List;

public abstract class ShuffleClient implements Cloneable
{
    public abstract List<PartitionLocation> registerShuffle(
        String applicationId,
        int shuffleId,
        int numMappers,
        int numPartitions
    );

    public abstract PartitionLocation revive(
        String applicationId,
        int shuffleId
    );

    public abstract void pushData(byte[] data, PartitionLocation location);

    public abstract void unregisterShuffle(String applicationId, int shuffleId);

    public abstract InputStream readPartition(
        String applicationId,
        int shuffleId,
        int reduceId
    );
}
