package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.protocol.PartitionLocation;

public class PartitionLocationWithDoubleChunks extends PartitionLocation {
    transient private DoubleChunk doubleChunk;

    public PartitionLocationWithDoubleChunks(
        PartitionLocation partitionLocation,
        DoubleChunk doubleChunk
    ) {
        super(partitionLocation);
        this.doubleChunk = doubleChunk;
    }

    public DoubleChunk getDoubleChunk() {
        return doubleChunk;
    }
}
