package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MemoryPool {
    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    private long capacity;
    private long chunkSize;
    private long memoryPoolAddress;
    private long[] startAddresses;
    private boolean[] empty;
    private int numSlots;
    private FlushBuffer[] flushBuffers;

    public MemoryPool(long capacity, long chunkSize) {
        this.capacity = capacity;
        this.chunkSize = chunkSize;
        memoryPoolAddress = Platform.allocateMemory(capacity);
        logger.info("allocated memory, size " + capacity);
        numSlots = (int) (capacity / chunkSize);
        startAddresses = new long[numSlots];
        empty = new boolean[numSlots];
        long curAddress = memoryPoolAddress;
        flushBuffers = new FlushBuffer[numSlots];
        for (int i = 0; i < numSlots; i++) {
            empty[i] = true;
            startAddresses[i] = curAddress;
            curAddress += chunkSize;

            flushBuffers[i] = new FlushBuffer(i, startAddresses[i], curAddress);
        }
    }

    /**
     * @return allocated address, null if failed
     */
    public FlushBuffer allocateChunk() {
        for (int i = 0; i < numSlots; i++) {
            if (empty[i]) {
                empty[i] = false;
                return new FlushBuffer(i, startAddresses[i], startAddresses[i] + chunkSize);
            }
        }
        return null;
    }

    public void returnChunk(FlushBuffer flushBuffer) {
        empty[flushBuffer.getId()] = true;
        flushBuffer.reset();
    }

    public ArrayList<FlushBuffer> allocateChunks(int size) {
        ArrayList<FlushBuffer> ret = new ArrayList<>();
        int retIndx = 0;
        for (int i = 0; i < numSlots; i++) {
            if (retIndx == size) {
                break;
            }
            if (empty[i]) {
                empty[i] = false;
                ret.add(flushBuffers[i]);
                if (ret.get(retIndx) == null) {
                    logger.error("Chunk is NULL!, i " + i + " numSlots " + numSlots);
                }
                retIndx++;
            }
        }

        return ret;
    }

    public void returnChunks(FlushBuffer[] flushBuffers) {
        for (int i = 0; i < flushBuffers.length; i++) {
            empty[flushBuffers[i].getId()] = true;
            flushBuffers[i].reset();
        }
    }

    public void returnChunks(ArrayList<FlushBuffer> flushBuffers) {
        for (int i = 0; i < flushBuffers.size(); i++) {
            empty[flushBuffers.get(i).getId()] = true;
            flushBuffers.get(i).reset();
        }
    }
}
