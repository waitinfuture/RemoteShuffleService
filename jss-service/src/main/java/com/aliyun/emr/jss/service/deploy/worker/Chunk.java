package com.aliyun.emr.jss.service.deploy.worker;

import com.aliyun.emr.jss.unsafe.Platform;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class Chunk {
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
        return (int)(endAddress - currentAddress + 1);
    }

    public void append(byte[] data) {
        Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, null, startAddress, data.length);
        currentAddress += data.length;
    }

    public void append(ByteBuf data) {
        int numBytes = data.readableBytes();
        for (int i = 0; i < numBytes; i++) {
            Platform.putByte(null, currentAddress, data.readByte());
            currentAddress++;
        }
    }

    public void clear() {
        currentAddress = startAddress;
    }

    public boolean flushData(OutputStream ostream) {
        return flushData(ostream, true);
    }
    /**
     *
     * @param ostream
     * @param flush whether to flush or just clear buffer
     * @return
     */
    public boolean flushData(OutputStream ostream, boolean flush) {
        try {
            if (flush) {
                for (long addr = startAddress; addr < currentAddress; addr++) {
                    ostream.write(Platform.getByte(null, addr));
                    ostream.flush();
                }
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

    public int getId() {
        return id;
    }
}
