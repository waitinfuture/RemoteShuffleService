package com.aliyun.emr.jss.service.deploy.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DoubleChunk {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);

    Chunk[] chunks = new Chunk[2];
    int working;
    MemoryPool memoryPool;
    String fileName;
    ChunkState slaveState = ChunkState.Ready;

    enum ChunkState {
        Ready, Flushing;
    }

    public DoubleChunk(Chunk ch1, Chunk ch2, MemoryPool memoryPool, String fileName) {
        chunks[0] = ch1;
        chunks[1] = ch2;
        this.memoryPool = memoryPool;
        this.fileName = fileName;
        working = 0;
    }

    /**
     * assume data size is less than chunk size
     * @param data
     * @return
     */
    public boolean append(byte[] data) {
        synchronized (this) {
            if (chunks[working].remaining() > data.length) {
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
                            chunks[(working + 1) % 2].flushData(ostream);
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
                    chunks[working].flushData(ostream);
                    chunks[working].append(data);
                } catch (IOException e) {
                    logger.error("create OutputStream failed!", e);
                    return false;
                }
                return true;
            }
        }
    }

    public boolean flush() {
        synchronized (this) {
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
