package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.unsafe.Platform;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;

public final class Chunk extends MinimalByteBuf {
    private static final Logger logger = LoggerFactory.getLogger(Chunk.class);

    private int id;
    private long startAddress;
    private long endAddress;
    private long currentAddress;

    public Chunk(int id, long startAddress, long endAddress) {
        this.id = id;
        this.startAddress = startAddress;
        this.endAddress = endAddress;
        this.currentAddress = startAddress;
    }

    public int remaining() {
        return (int)(endAddress - currentAddress);
    }

    public void append(byte[] data) {
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, null, currentAddress, data.length);
        currentAddress += data.length;
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

    public boolean flushData(DataOutputStream ostream) {
        return flushData(ostream, true);
    }
    /**
     *
     * @param ostream
     * @param flush whether to flush or just clear buffer
     * @return
     */
    public boolean flushData(DataOutputStream ostream, boolean flush) {
        try {
            if (flush) {
                byte[] data = new byte[(int)(currentAddress - startAddress)];
                Platform.copyMemory(null, startAddress, data, Platform.BYTE_ARRAY_OFFSET, data.length);
                ostream.write(data);
                ostream.flush();
            }
            currentAddress = startAddress;
            return true;
        } catch (IOException e) {
            logger.error("flush data failed", e);
        }
        return false;
    }

    public byte[] toBytes() {
        byte[] data = new byte[(int)(currentAddress - startAddress)];
        for (long addr = startAddress; addr < currentAddress; addr++) {
            data[(int)(addr - startAddress)] = Platform.getByte(null, addr);
        }
        return data;
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
