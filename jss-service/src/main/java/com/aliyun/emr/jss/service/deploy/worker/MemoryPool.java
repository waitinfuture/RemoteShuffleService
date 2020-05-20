package com.aliyun.emr.jss.service.deploy.worker;

import com.aliyun.emr.jss.unsafe.Platform;

public class MemoryPool {
    private long capacity;
    private long chunkSize;
    private long memoryPoolAddress;
    private long[] startAddresses;
    private boolean[] empty;
    private int numSlots;

    public MemoryPool(long capacity, long chunkSize) {
        this.capacity = capacity;
        this.chunkSize = chunkSize;
        memoryPoolAddress = Platform.allocateMemory(capacity);
        numSlots = (int)(capacity / chunkSize);
        startAddresses = new long[numSlots];
        empty = new boolean[numSlots];
        long curAddress = memoryPoolAddress;
        for (int i = 0; i < numSlots; i++) {
            empty[i] = true;
            startAddresses[i] = curAddress;
            curAddress += chunkSize;
        }
    }

    /**
     * @return allocated address, null if failed
     */
    public Chunk allocateChunk() {
        synchronized (this) {
            for (int i = 0; i < numSlots; i++) {
                if (empty[i]) {
                    empty[i] = false;
                    return new Chunk(i, startAddresses[i], startAddresses[i] + chunkSize);
                }
            }
            return null;
        }
    }

    public void returnChunk(Chunk chunk) {
        synchronized (this) {
            empty[chunk.getId()] = true;
        }
    }
}
