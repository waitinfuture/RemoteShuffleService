package com.aliyun.emr.ess.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class DoubleChunk {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);
    private ExecutorService flushExecutorService;

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
    FSDataOutputStream ostream = null;

    public enum ChunkState {
        Ready, Flushing;
    }

    public DoubleChunk(
        Chunk ch1,
        Chunk ch2,
        MemoryPool memoryPool,
        Path fileName,
        FileSystem fs,
        ExecutorService executorService) {
        chunks[0] = ch1;
        chunks[1] = ch2;
        if (ch1 == null) {
            logger.error("ch1 is NULL!");
        }
        if (ch2 == null) {
            logger.error("ch2 is NULL!");
        }
        this.memoryPool = memoryPool;
        this.fileName = fileName;
        working = 0;
        this.fs = fs;
        this.flushExecutorService = executorService;
    }

    public void initWithData(int working, byte[] masterData, byte[] slaveData) {
        this.working = working;
        chunks[working].reset();
        chunks[working].append(masterData);
        chunks[(working + 1) % 2].reset();
        chunks[(working + 1) % 2].append(slaveData);
    }

    public boolean append(byte[] data) {
        return append(data, true);
    }

    public boolean append(byte[] data, boolean flush) {
        if (flushed) {
            logger.error("already flushed!");
            return false;
        }
        synchronized (this) {
            if (chunks[working].remaining() >= data.length) {
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
                            if (ostream == null) {
                                ostream = fs.create(fileName);
                            }
                            chunks[slaveIndex].flushData(ostream, flush);
                            slaveState = ChunkState.Ready;
                        } catch (Exception e) {
                            logger.error("create OutputStream failed!", e);
                        }
                    }
                };
                flushThread.start();
            } else {
                chunks[slaveIndex].reset();
            }
            return true;
        }
    }

    public boolean append(ByteBuf data) {
        return append(data, true);
    }

    /**
     * assume data size is less than chunk capacity
     *
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
                // async flush the full chunk
                slaveState = ChunkState.Flushing;
                flushExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (ostream == null) {
                                ostream = fs.create(fileName);
                            }
                            chunks[slaveIndex].flushData(ostream, flush);
                            slaveState = ChunkState.Ready;
                        } catch (Exception e) {
                            logger.error("create OutputStream failed!", e);
                        }
                    }
                });
            } else {
                chunks[slaveIndex].reset();
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
                    logger.info("Wait slave finish flushing " + fileName);
                    Thread.sleep(50);
                } catch (Exception e) {
                    logger.error("sleep throws Exception", e);
                    return false;
                }
            }
            masterState = ChunkState.Flushing;
            try {
                // flush master chunk
                if (chunks[working].hasData()) {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
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
        synchronized (memoryPool) {
            memoryPool.returnChunk(chunks[0]);
            memoryPool.returnChunk(chunks[1]);
        }
    }
}
