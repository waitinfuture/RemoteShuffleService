package com.aliyun.emr.network.protocol;

import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public final class PushData extends AbstractMessage implements RequestMessage {
    public long requestId;

    public int epoch;

    // 0 for master, 1 for slave, see PartitionLocation.Mode
    public final byte mode;

    public final String shuffleKey;
    public final String partitionUniqueId;

    public PushData(byte mode, String shuffleKey, String partitionId, ManagedBuffer body) {
        this(0L, 0, mode, shuffleKey, partitionId, body);
    }

    public PushData(int epoch, byte mode, String shuffleKey, String partitionId, ManagedBuffer body) {
        this(0L, epoch, mode, shuffleKey, partitionId, body);
    }

    private PushData(long requestId, int epoch, byte mode, String shuffleKey, String partitionUniqueId,
                     ManagedBuffer body) {
        super(body, true);
        this.requestId = requestId;
        this.epoch = epoch;
        this.mode = mode;
        this.shuffleKey = shuffleKey;
        this.partitionUniqueId = partitionUniqueId;
    }

    @Override
    public Type type() {
        return Type.PushData;
    }

    @Override
    public int encodedLength() {
        return 8 + 4 + 1 + Encoders.Strings.encodedLength(shuffleKey) +
                Encoders.Strings.encodedLength(partitionUniqueId);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        buf.writeInt(epoch);
        buf.writeByte(mode);
        Encoders.Strings.encode(buf, shuffleKey);
        Encoders.Strings.encode(buf, partitionUniqueId);
    }

    public static PushData decode(ByteBuf buf) {
        long requestId = buf.readLong();
        int epoch = buf.readInt();
        byte mode = buf.readByte();
        String shuffleKey = Encoders.Strings.decode(buf);
        String partitionId = Encoders.Strings.decode(buf);
        return new PushData(requestId, epoch, mode, shuffleKey, partitionId,
                new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, epoch, mode, shuffleKey, partitionUniqueId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PushData) {
            PushData o = (PushData) other;
            return requestId == o.requestId && epoch == o.epoch && mode == o.mode
                    && shuffleKey.equals(o.shuffleKey) && partitionUniqueId.equals((o.partitionUniqueId))
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
                .add("partitionId", partitionUniqueId)
                .add("body size", body().size())
                .toString();
    }
}
