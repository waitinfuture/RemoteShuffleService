package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.protocol.PartitionLocation;

public class WorkingPartition extends PartitionLocation {
    final transient private FileWriter fileWriter;

    public WorkingPartition(
        PartitionLocation partitionLocation,
        FileWriter fileWriter) {
        super(partitionLocation);
        this.fileWriter = fileWriter;
    }

    public FileWriter getFileWriter() {
        return fileWriter;
    }
}
