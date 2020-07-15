package com.aliyun.emr.ess.service.deploy.worker;

import io.netty.buffer.ByteBuf;
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

    private final FileSystem fs;
    private FSDataOutputStream ostream = null;

    private long epoch;

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

    /**
     * assume data size is less than chunk capacity
     *
     * @param data
     * @return current epoch
     */
    public long append(ByteBuf data) throws Exception {
        if (flushed) {
            String msg = "already flushed!";
            logger.error(msg);
            throw new Exception(msg);
        }
        synchronized (this) {
            if (chunks[working].remaining() >= data.readableBytes()) {
                chunks[working].append(data);
                return epoch;
            }
            // if slave is flushing, wait for slave finish flushing
            while (slaveState == ChunkState.Flushing) {
                logger.info("slave chunk is flushing, wait for slave to finish...");
                Thread.sleep(50);
            }
            // now slave is empty, switch to slave and append data
            epoch++;
            working = (working + 1) % 2;
            final int slaveIndex = (working + 1) % 2;

            chunks[working].append(data);

            // async flush the full chunk
            slaveState = ChunkState.Flushing;
            flushExecutorService.submit(() -> {
                try {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
                    }
                    chunks[slaveIndex].flushData(ostream, true);
                    slaveState = ChunkState.Ready;
                } catch (Exception e) {
                    logger.error("create OutputStream failed!", e);
                }
            });

            return epoch;
        }
    }

    /**
     * assume data size is less than chunk capacity
     *
     * @param data
     * @return current epoch
     */
    public long append(ByteBuf data, long dataEpoch) throws Exception {
        if (flushed) {
            String msg = "already flushed!";
            logger.error(msg);
            throw new Exception(msg);
        }
        synchronized (this) {
            if (dataEpoch < epoch) {
                logger.debug(String.format("drop data of epoch %s, current epoch %s", dataEpoch, epoch));
                return epoch;
            }

            if (dataEpoch > epoch) {
                logger.debug(String.format("new epoch %s, current epoch %s", dataEpoch, epoch));
                epoch = dataEpoch;
                working = (working + 1) % 2;
                final int slaveIndex = (working + 1) % 2;
                chunks[slaveIndex].reset();
            }

            if (chunks[working].remaining() < data.readableBytes()) {
                String msg = "no space!";
                logger.error(msg);
                throw new Exception(msg);
            }

            chunks[working].append(data);

            return epoch;
        }
    }

    /**
     *
     * @return 0: success and file not empty; 1: file empty; -1: fail
     */
    public int flush() {
        if (flushed) {
            logger.error("already flushed!");
            return -1;
        }
        synchronized (this) {
            // wait for flush slave chunk to finish
            while (slaveState == ChunkState.Flushing) {
                try {
                    logger.info("Wait slave finish flushing " + fileName);
                    Thread.sleep(50);
                } catch (Exception e) {
                    logger.error("sleep throws Exception", e);
                    return -1;
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
                } else if (ostream == null) {
                    return 1;
                }
            } catch (IOException e) {
                logger.error("flush data failed!", e);
                return -1;
            }
            masterState = ChunkState.Ready;
            flushed = true;
            return 0;
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
