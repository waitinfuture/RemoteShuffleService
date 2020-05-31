package com.aliyun.emr.jss.protocol.message;

public enum ReturnCode {
    Success,
    PartialSuccess,
    ShuffleNotRegistered,
    WorkerNotFound,
    SlotNotAvailable,
    ReserveBufferFailed,
    PartitionNotFound,
    MasterPartitionNotFound,
    SlavePartitionNotFound,
    DeleteFilesFailed,
    PartitionExists,
    Failed
}
