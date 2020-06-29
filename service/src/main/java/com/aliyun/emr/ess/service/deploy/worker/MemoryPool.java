package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPool {
    private static final Logger logger = LoggerFactory.getLogger(DoubleChunk.class);

    private long capacity;
    private long chunkSize;
    private long memoryPoolAddress;
    private long[] startAddresses;
    private boolean[] empty;
    private int numSlots;
    private Chunk[] chunks;

    public MemoryPool(long capacity, long chunkSize) {
        this.capacity = capacity;
        this.chunkSize = chunkSize;
        memoryPoolAddress = Platform.allocateMemory(capacity);
        logger.info("allocated memory, size " + capacity);
        numSlots = (int) (capacity / chunkSize);
        startAddresses = new long[numSlots];
        empty = new boolean[numSlots];
        long curAddress = memoryPoolAddress;
        chunks = new Chunk[numSlots];
        for (int i = 0; i < numSlots; i++) {
            empty[i] = true;
            startAddresses[i] = curAddress;
            curAddress += chunkSize;

            chunks[i] = new Chunk(i, startAddresses[i], curAddress);
        }
    }

    /**
     * @return allocated address, null if failed
     */
    public Chunk allocateChunk() {
        for (int i = 0; i < numSlots; i++) {
            if (empty[i]) {
                empty[i] = false;
                return new Chunk(i, startAddresses[i], startAddresses[i] + chunkSize);
            }
        }
        return null;
    }

    public void returnChunk(Chunk chunk) {
        empty[chunk.getId()] = true;
        chunk.reset();
    }

    public Chunk[] allocateChunks(int size) {
        Chunk[] ret = new Chunk[size];
        int retIndx = 0;
        for (int i = 0; i < numSlots; i++) {
            if (retIndx == size) {
                break;
            }
            if (empty[i]) {
                empty[i] = false;
                ret[retIndx] = chunks[i];
                if (ret[retIndx] == null) {
                    logger.error("Chunk is NULL!, i " + i + " numSlots " + numSlots);
                }
                retIndx++;
            }
        }

        return ret;
    }

    public void returnChunks(Chunk[] chunks) {
        for (int i = 0; i < chunks.length; i++) {
            empty[chunks[i].getId()] = true;
            chunks[i].reset();
        }
    }
}
