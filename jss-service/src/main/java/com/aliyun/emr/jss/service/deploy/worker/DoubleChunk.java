package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DoubleChunk {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);

    // exposed for test
    public Chunk[] chunks = new Chunk[2];
    // exposed for test
    public int working;
    MemoryPool memoryPool;
    // exposed for test
    public Path fileName;
    // exposed for test
    public ChunkState slaveState = ChunkState.Ready;
    public ChunkState masterState = ChunkState.Ready;
    // exposed for test
    public boolean flushed = false;
    FileSystem fs;

    public enum ChunkState {
        Ready, Flushing;
    }

    public DoubleChunk(Chunk ch1, Chunk ch2, MemoryPool memoryPool, Path fileName, FileSystem fs)
    {
        chunks[0] = ch1;
        chunks[1] = ch2;
        this.memoryPool = memoryPool;
        this.fileName = fileName;
        working = 0;
        this.fs = fs;
    }

    public void initWithData(int working, byte[] masterData, byte[] slaveData) {
        this.working = working;
        chunks[working].clear();
        chunks[working].append(masterData);
        chunks[(working + 1) % 2].clear();
        chunks[(working + 1) % 2].append(slaveData);
    }

    public boolean append(byte[] data) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        return append(Unpooled.copiedBuffer(data), true);
    }

    public boolean append(byte[] data, boolean flush) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        return append(Unpooled.copiedBuffer(data), flush);
    }

    public boolean append(ByteBuf data) {
        return append(data, true);
    }

    /**
     * assume data size is less than chunk capacity
     * @param data
     * @param flush whether to flush or just abandon
     * @return
     */
    public boolean append(ByteBuf data, boolean flush) {
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
                    Thread.sleep(50);
                } catch (Exception e) {
                    logger.error("sleep throws Exception", e);
                    return false;
                }
            }
            // now slave is empty, switch to slave and append data
            working = (working + 1) % 2;
            int slaveIndex = (working + 1) % 2;
            chunks[working].append(data);
            if (flush) {
                // create new thread to flush the full chunk
                slaveState = ChunkState.Flushing;
                Thread flushThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            if (!fs.exists(fileName)) {
                                fs.createNewFile(fileName);
                            }

                            FSDataOutputStream ostream = fs.append(fileName);
                            chunks[slaveIndex].flushData(ostream, flush);
                            ostream.close();
                            slaveState = ChunkState.Ready;
                        } catch (Exception e) {
                            logger.error("create OutputStream failed!", e);
                        }
                    }
                };
                flushThread.start();
            } else {
                chunks[slaveIndex].clear();
            }
            return true;
        }
    }

    public boolean flush() {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        synchronized (this) {
            // wait for flush slave chunk to finish
            while (slaveState == ChunkState.Flushing) {
                try {
                    Thread.sleep(50);
                } catch(Exception e) {
                    logger.error("sleep throws Exception", e);
                    return false;
                }
            }
            masterState = ChunkState.Flushing;
            try {
                // flush master chunk
                if (chunks[working].hasData()) {
                    if (!fs.exists(fileName)) {
                        fs.createNewFile(fileName);
                    }
                    FSDataOutputStream ostream = null;
                    boolean getLease = false;
                    while (!getLease) {
                        try {
                            ostream = fs.append(fileName);
                            getLease = true;
                        } catch (Exception e) {
                            logger.warn(String.format("append %s failed, try again", fileName
                                .toString()), e);
                            try {
                                Thread.sleep(100);
                            } catch (Exception ex) {
                                logger.warn("Sleep caught exception");
                            }
                        }
                    }
                    chunks[working].flushData(ostream);
                    ostream.close();
                }
            } catch (IOException e) {
                logger.error("flush data failed!", e);
                return false;
            }
            masterState = ChunkState.Ready;
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
