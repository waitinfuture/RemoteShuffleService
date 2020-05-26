package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Transient;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public class DoubleChunk implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);

    transient Chunk[] chunks = new Chunk[2];
    int working;
    transient MemoryPool memoryPool;
    String fileName;
    // exposed for test
    public ChunkState slaveState = ChunkState.Ready;

    public enum ChunkState {
        Ready, Flushing;
    }

    public DoubleChunk(Chunk ch1, Chunk ch2, MemoryPool memoryPool, String fileName) {
        chunks[0] = ch1;
        chunks[1] = ch2;
        this.memoryPool = memoryPool;
        this.fileName = fileName;
        working = 0;
    }

    public synchronized boolean append(byte[] data) {
        return append(Unpooled.copiedBuffer(data), true);
    }

    public synchronized boolean append(byte[] data, boolean flush) {
        return append(Unpooled.copiedBuffer(data), flush);
    }

    public synchronized boolean append(ByteBuf data) {
        return append(data, true);
    }

    /**
     * assume data size is less than chunk capacity
     * @param data
     * @param flush whether to flush or just abandon
     * @return
     */
    public synchronized  boolean append(ByteBuf data, boolean flush) {
        synchronized (this) {
            if (chunks[working].remaining() > data.readableBytes()) {
                chunks[working].append(data);
                return true;
            } else if (slaveState == ChunkState.Ready){
                // slave is empty, switch to slave
                working = (working + 1) % 2;
                chunks[working].append(data);
                slaveState = ChunkState.Flushing;
                Thread flushThread = new Thread() {
                    public void run() {
                        try {
                            // TODO: construct output stream
                            OutputStream ostream = new FileOutputStream(fileName, true);
                            chunks[(working + 1) % 2].flushData(ostream, flush);
                            slaveState = ChunkState.Ready;
                        } catch (Exception e) {
                            logger.error("create OutputStream failed!", e);
                        }
                    }
                };
                flushThread.start();
                return true;
            } else {
                // slave is flusing, just flush, do not switch
                logger.info("slave chunk is flushing, just flush data");
                try {
                    // TODO: construct output stream
                    OutputStream ostream = new FileOutputStream(fileName, true);
                    chunks[working].flushData(ostream, flush);
                    chunks[working].append(data);
                } catch (IOException e) {
                    logger.error("create OutputStream failed!", e);
                    return false;
                }
                return true;
            }
        }
    }

    public synchronized boolean flush() {
        synchronized (this) {
            // wait for flush slave chunk to finish
            while (slaveState == ChunkState.Flushing) {
                try {
                    Thread.sleep(500);
                } catch(Exception e) {
                    logger.error("sleep throws Exception", e);
                    return false;
                }
            }
            // TODO construct OutputStream
            try {
                // flush master chunk
                OutputStream ostream = new FileOutputStream(fileName, true);
                chunks[working].flushData(ostream);
            } catch (IOException e) {
                logger.error("construct outputstream failed!", e);
                return false;
            }
            return true;
        }
    }

    public void returnChunks() {
        memoryPool.returnChunk(chunks[0]);
        memoryPool.returnChunk(chunks[1]);
    }
}
