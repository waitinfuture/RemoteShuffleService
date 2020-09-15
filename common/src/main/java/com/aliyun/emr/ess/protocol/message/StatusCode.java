package com.aliyun.emr.ess.protocol.message;

public enum StatusCode {
    // 1/0 Status
    Success(0),
    PartialSuccess(1),
    Failed(2),

    // Specific Status
    ShuffleAlreadyRegistered(3),
    ShuffleNotRegistered(4),
    ReserveBufferFailed(5),
    SlotNotAvailable(6),
    WorkerNotFound(7),
    PartitionNotFound(8),
    SlavePartitionNotFound(9),
    DeleteFilesFailed(10),
    PartitionExists(11),
    ReviveFailed(12),
    PushDataFailed(13),
    ReplicateDataFailed(14),
    NumMapperZero(15),
    MapEnded(16),
    StageEnded(17);

    private byte value;
    StatusCode(int value) {
        assert(value >= 0 && value < 256);
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }
}