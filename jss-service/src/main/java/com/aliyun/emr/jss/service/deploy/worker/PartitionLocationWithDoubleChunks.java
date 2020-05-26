package com.aliyun.emr.jss.service.deploy.worker;

import com.aliyun.emr.jss.protocol.PartitionLocation;

public class PartitionLocationWithDoubleChunks extends PartitionLocation {
    private DoubleChunk doubleChunk;

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
