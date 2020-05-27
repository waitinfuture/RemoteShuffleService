package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public class DoubleChunk implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);

    // exposed for test
    transient public Chunk[] chunks = new Chunk[2];
    // exposed for test
    public int working;
    transient MemoryPool memoryPool;
    // exposed for test
    public String fileName;
    // exposed for test
    public ChunkState slaveState = ChunkState.Ready;
    // exposed for test
    public boolean flushed = false;

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

    public void initWithData(int working, byte[] masterData, byte[] slaveData) {
        this.working = working;
        chunks[working].clear();
        chunks[working].append(masterData);
        chunks[(working + 1) % 2].clear();
        chunks[(working + 1) % 2].append(slaveData);
    }

    public synchronized boolean append(byte[] data) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        return append(Unpooled.copiedBuffer(data), true);
    }

    public synchronized boolean append(byte[] data, boolean flush) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        return append(Unpooled.copiedBuffer(data), flush);
    }

    public synchronized boolean append(ByteBuf data) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        return append(data, true);
    }

    /**
     * assume data size is less than chunk capacity
     * @param data
     * @param flush whether to flush or just abandon
     * @return
     */
    public synchronized  boolean append(ByteBuf data, boolean flush) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        synchronized (this) {
            if (chunks[working].remaining() >= data.readableBytes()) {
                chunks[working].append(data);
                return true;
            }
            // if slave is flusing, wait for slave finish flushing
            while (slaveState == ChunkState.Flushing) {
                logger.info("slave chunk is flushing, wait for slave to finish...");
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    logger.error("sleep throws Exception", e);
                    return false;
                }
            }
            // now slave is empty, switch to slave and append data
            working = (working + 1) % 2;
            chunks[working].append(data);
            // create new thread to flush the full chunk
            slaveState = ChunkState.Flushing;
            Thread flushThread = new Thread() {
                public void run() {
                    try {
                        // TODO: construct output stream
                        OutputStream ostream = new FileOutputStream(fileName, true);
                        chunks[(working + 1) % 2].flushData(ostream, flush);
                        ostream.close();
                        // for test
//                        Thread.sleep(2000);
                        slaveState = ChunkState.Ready;
                    } catch (Exception e) {
                        logger.error("create OutputStream failed!", e);
                    }
                }
            };
            flushThread.start();
            return true;
        }
    }

    public synchronized boolean flush() {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
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
                ostream.close();
            } catch (IOException e) {
                logger.error("construct outputstream failed!", e);
                return false;
            }
            flushed = true;
            return true;
        }
    }

    public byte[] getMasterData() {
        return chunks[working].toBytes();
    }

    public byte[] getSlaveData() {
        return chunks[(working + 1) % 2].toBytes();
    }

    public void returnChunks() {
        memoryPool.returnChunk(chunks[0]);
        memoryPool.returnChunk(chunks[1]);
    }
}
