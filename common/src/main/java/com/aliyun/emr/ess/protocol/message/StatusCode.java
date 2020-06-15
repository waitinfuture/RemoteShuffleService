package com.aliyun.emr.ess.protocol.message;

public enum StatusCode
{
    // 1/0 Status
    Success,
    PartialSuccess,
    Failed,

    // Specific Status
    ShuffleAlreadyRegistered,
    ShuffleNotRegistered,
    ReserveBufferFailed,
    SlotNotAvailable,
    WorkerNotFound,
    PartitionNotFound,
    MasterPartitionNotFound,
    SlavePartitionNotFound,
    DeleteFilesFailed,
    PartitionExists,
    ReviveFailed,
    PushDataFailed,
    ReplicateDataFailed
}
