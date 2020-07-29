package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.unsafe.Platform;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlushBuffer extends MinimalByteBuf {
    private static final Logger logger = LoggerFactory.getLogger(FlushBuffer.class);

    private final int id;
    private final long startAddress;
    private final long endAddress;
    private long currentAddress;

    public FlushBuffer(int id, long startAddress, long endAddress) {
        this.id = id;
        this.startAddress = startAddress;
        this.endAddress = endAddress;
        this.currentAddress = startAddress;
    }

    public int remaining() {
        return (int)(endAddress - currentAddress);
    }

    public void append(ByteBuf data) {
        final int length = data.readableBytes();
        final int dstIndex = (int) (currentAddress - startAddress);
        data.getBytes(data.readerIndex(), this, dstIndex, length);
        currentAddress += length;
    }

    public void reset() {
        currentAddress = startAddress;
    }

    public boolean hasData() {
        return (currentAddress > startAddress);
    }

    public int getId() {
        return id;
    }

    public long getStartAddress() {
        return startAddress;
    }

    public long getEndAddress() {
        return endAddress;
    }

    public long getCurrentAddress() {
        return currentAddress;
    }

    @Override
    public int capacity() {
        return (int) (endAddress - startAddress);
    }

    @Override
    public boolean hasMemoryAddress() {
        return true;
    }

    @Override
    public long memoryAddress() {
        return startAddress;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        if (src.hasMemoryAddress()) {
            Platform.copyMemory(null, src.memoryAddress() + srcIndex,
                null, startAddress + index, length);
        } if (src.hasArray()) {
            Platform.copyMemory(
                src.array(), Platform.BYTE_ARRAY_OFFSET + src.arrayOffset() + srcIndex,
                null, startAddress + index, length);
        } else {
            src.getBytes(srcIndex, this, index, length);
        }
        return this;
    }
}
