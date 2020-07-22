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
    private final ExecutorService flushExecutorService;

    // exposed for test
    public Chunk[] chunks = new Chunk[2];
    // exposed for test
    public int activeIndex;
    MemoryPool memoryPool;
    // exposed for test
    public Path fileName;
    // exposed for test
    public ChunkState inactiveState = ChunkState.Ready;
    public ChunkState activeState = ChunkState.Ready;
    // exposed for test
    public volatile boolean closed = false;

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
        activeIndex = 0;
        this.fs = fs;
        this.flushExecutorService = executorService;
    }

    public void initWithData(int working, byte[] masterData, byte[] slaveData) {
        this.activeIndex = working;
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
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new Exception(msg);
        }
        synchronized (this) {
            if (chunks[activeIndex].remaining() >= data.readableBytes()) {
                chunks[activeIndex].append(data);
                return epoch;
            }
            // if slave is flushing, wait for inactive chunk finish flushing
            while (inactiveState == ChunkState.Flushing) {
                logger.info("slave chunk is flushing, wait for inactive chunk to finish...");
                Thread.sleep(50);
            }
            // now slave is empty, switch to inactive chunk and append data
            epoch++;
            activeIndex = (activeIndex + 1) % 2;
            final int inactiveIndex = (activeIndex + 1) % 2;

            chunks[activeIndex].append(data);

            // async flush the full chunk
            inactiveState = ChunkState.Flushing;
            flushExecutorService.submit(() -> {
                try {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
                    }
                    chunks[inactiveIndex].flushData(ostream, true);
                    inactiveState = ChunkState.Ready;
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
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new Exception(msg);
        }
        synchronized (this) {
            if (dataEpoch < epoch - 1) {
                logger.info(String.format("drop data of epoch %s, current epoch %s", dataEpoch, epoch));
                return epoch;
            }

            Chunk targetChunk;
            if (dataEpoch > epoch) {
                logger.info(String.format("new epoch %s, current epoch %s", dataEpoch, epoch));
                activeIndex = (activeIndex + 1) % 2;
                targetChunk = chunks[activeIndex];
                targetChunk.reset();
                if (dataEpoch > epoch + 1) {
                    final int inactiveIndex = (activeIndex + 1) % 2;
                    chunks[inactiveIndex].reset();
                }
                epoch = dataEpoch;
            } else if (dataEpoch == epoch) {
                targetChunk = chunks[activeIndex];
            } else if (dataEpoch == epoch - 1) {
                final int inactiveIndex = (activeIndex + 1) % 2;
                targetChunk = chunks[inactiveIndex];
            } else {
                throw new IllegalStateException("cannot reach here!");
            }

            if (targetChunk.remaining() < data.readableBytes()) {
                String msg = "no space!";
                logger.error(msg);
                throw new Exception(msg);
            }

            targetChunk.append(data);

            return epoch;
        }
    }

    /**
     *
     * @return 0: success and file not empty; 1: file empty; -1: fail
     */
    public int close(boolean isMasterMode) {
        if (isMasterMode) {
            return doMasterClose();
        } else {
            return doSlaveClose();
        }
    }

    private int doMasterClose() {
        if (closed) {
            logger.error("already closed!");
            return -1;
        }
        synchronized (this) {
            // wait for flush inactive chunk to finish
            while (inactiveState == ChunkState.Flushing) {
                try {
                    logger.info("Wait inactive chunk finish flushing " + fileName);
                    Thread.sleep(50);
                } catch (Exception e) {
                    logger.error("sleep throws Exception", e);
                    return -1;
                }
            }
            activeState = ChunkState.Flushing;
            try {
                // flush active chunk
                if (chunks[activeIndex].hasData()) {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
                    }

                    chunks[activeIndex].flushData(ostream);
                    ostream.close();
                } else if (ostream == null) {
                    return 1;
                }
            } catch (IOException e) {
                logger.error("flush data failed!", e);
                return -1;
            }
            activeState = ChunkState.Ready;
            closed = true;
            return 0;
        }
    }

    private int doSlaveClose() {
        if (closed) {
            logger.error("already closed!");
            return -1;
        }
        synchronized (this) {
            inactiveState = ChunkState.Flushing;
            activeState = ChunkState.Flushing;
            try {
                // flush inactive chunk
                final int inactiveIndex = (activeIndex + 1) % 2;
                if (chunks[inactiveIndex].hasData()) {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
                    }
                    chunks[inactiveIndex].flushData(ostream);
                }

                // flush active chunk
                if (chunks[activeIndex].hasData()) {
                    if (ostream == null) {
                        ostream = fs.create(fileName);
                    }
                    chunks[activeIndex].flushData(ostream);
                }

                if (ostream == null) {
                    return 1;
                } else {
                    ostream.close();
                }
            } catch (IOException e) {
                logger.error("flush data failed!", e);
                return -1;
            }

            inactiveState = ChunkState.Ready;
            activeState = ChunkState.Ready;
            closed = true;
            return 0;
        }
    }

    public byte[] getActiveData() {
        return chunks[activeIndex].toBytes();
    }

    public byte[] getInactiveData() {
        return chunks[(activeIndex + 1) % 2].toBytes();
    }

    public void returnChunks() {
        synchronized (memoryPool) {
            memoryPool.returnChunk(chunks[0]);
            memoryPool.returnChunk(chunks[1]);
        }
    }
}
