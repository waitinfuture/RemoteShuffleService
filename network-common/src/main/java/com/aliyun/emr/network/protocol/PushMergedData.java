package com.aliyun.emr.network.protocol;

import java.util.Arrays;

import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public final class PushMergedData extends AbstractMessage implements RequestMessage {
    public long requestId;

    // 0 for master, 1 for slave, see PartitionLocation.Mode
    public final byte mode;

    public final String shuffleKey;
    public final String[] partitionUniqueIds;
    public final int[] batchOffsets;

    public PushMergedData(
        byte mode,
        String shuffleKey,
        String[] partitionIds,
        int[] batchOffsets,
        ManagedBuffer body) {
        this(0L, mode, shuffleKey, partitionIds, batchOffsets, body);
    }

    private PushMergedData(
        long requestId,
        byte mode,
        String shuffleKey,
        String[] partitionUniqueIds,
        int[] batchOffsets,
        ManagedBuffer body) {
        super(body, true);
        this.requestId = requestId;
        this.mode = mode;
        this.shuffleKey = shuffleKey;
        this.partitionUniqueIds = partitionUniqueIds;
        this.batchOffsets = batchOffsets;
    }

    @Override
    public Type type() {
        return Type.PushMergedData;
    }

    @Override
    public int encodedLength() {
        return 8 + 1 + Encoders.Strings.encodedLength(shuffleKey) +
                Encoders.StringArrays.encodedLength(partitionUniqueIds) +
                Encoders.IntArrays.encodedLength(batchOffsets);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        buf.writeByte(mode);
        Encoders.Strings.encode(buf, shuffleKey);
        Encoders.StringArrays.encode(buf, partitionUniqueIds);
        Encoders.IntArrays.encode(buf, batchOffsets);
    }

    public static PushMergedData decode(ByteBuf buf) {
        long requestId = buf.readLong();
        byte mode = buf.readByte();
        String shuffleKey = Encoders.Strings.decode(buf);
        String[] partitionIds = Encoders.StringArrays.decode(buf);
        int[] batchOffsets = Encoders.IntArrays.decode(buf);
        return new PushMergedData(requestId, mode, shuffleKey, partitionIds, batchOffsets,
            new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
            requestId, mode, shuffleKey, partitionUniqueIds, batchOffsets, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PushMergedData) {
            PushMergedData o = (PushMergedData) other;
            return requestId == o.requestId && mode == o.mode && shuffleKey.equals(o.shuffleKey)
                && Arrays.equals(partitionUniqueIds, o.partitionUniqueIds)
                && Arrays.equals(batchOffsets, o.batchOffsets)
                && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("requestId", requestId)
                .add("mode", mode)
                .add("shuffleKey", shuffleKey)
                .add("partitionIds", Arrays.toString(partitionUniqueIds))
                .add("batchOffsets", Arrays.toString(batchOffsets))
                .add("body size", body().size())
                .toString();
    }
}
