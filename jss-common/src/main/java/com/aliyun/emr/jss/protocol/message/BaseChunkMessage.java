package com.aliyun.emr.jss.protocol.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.protocol.Encodable;

import java.nio.ByteBuffer;

public abstract class BaseChunkMessage implements Encodable
{
    protected abstract Type type();

    public enum Type {
        OPEN_CHUNKS(0), APPEND_CHUNK(1);

        private final byte id;

        Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() { return id; }
    }

    // NB: Java does not support static methods in interfaces, so we must put this in a static class.
    public static class Decoder {
        /** Deserializes the 'type' byte followed by the message itself. */
        public static BaseChunkMessage fromByteBuffer(ByteBuffer msg) {
            ByteBuf buf = Unpooled.wrappedBuffer(msg);
            byte type = buf.readByte();
            switch (type) {
                case 0: return OpenChunks.decode(buf);
                case 1: return AppendChunk.decode(buf);
                default: throw new IllegalArgumentException("Unknown message type: " + type);
            }
        }
    }

    /** Serializes the 'type' byte followed by the message itself. */
    public ByteBuffer toByteBuffer() {
        // Allow room for encoded message, plus the type byte
        ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
        buf.writeByte(type().id);
        encode(buf);
        assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
        return buf.nioBuffer();
    }
}
